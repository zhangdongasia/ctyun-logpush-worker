/**
 * Cloudflare Workers — Logpush Backfill Worker
 *
 * 补传指定时间范围 [BACKFILL_START_TIME, BACKFILL_END_TIME] 的 Logpush 原始日志。
 * 独立部署，和生产 worker(ctyun-logpush)并存，共享 R2 bucket 和 parse-queue。
 *
 * 两阶段流程：
 *   Phase 1 (Cleanup): 扫描 processed/ 前缀，删除该时间范围内已存在的 batch 文件
 *                      (.txt) 和 Sender 幂等标记 (.txt.done)。
 *                      关键原因：如果不清 .done，Sender 会"跳过重复"，导致客户收不到补传；
 *                                如果不清 .txt，可能留下孤儿文件(新 batch 数少于旧 batch 数时)。
 *   Phase 2 (Enqueue): 按日期 prefix 扫描 logs/YYYYMMDD/，过滤时间范围匹配的 raw 文件，
 *                      以 rate 为上限逐分钟入队 parse-queue。下游复用生产 worker 的 Parser
 *                      和 Sender 逻辑，自然走完后续流程。
 *
 * 特性：
 *   - Cron 每分钟触发一次，幂等、可中断、可继续
 *   - Rate limiting 避免打爆客户接收端（默认 10 files/min，最大 100）
 *   - State 保存在 R2 backfill-state/progress.json，HTTP /backfill/status 可查
 *   - 配置变更自动重置 state（改 BACKFILL_START/END_TIME 后 redeploy 即可重跑）
 *   - BACKFILL_ENABLED=false 可暂停
 *   - 48h 范围上限（防误操作）
 *
 * 部署：wrangler deploy --config wrangler_backfill.toml
 * 删除：wrangler delete --config wrangler_backfill.toml
 *
 * Env Secrets（无需 — 不直接调接收端，仅入队）
 * Env Vars     : BACKFILL_START_TIME, BACKFILL_END_TIME, BACKFILL_RATE,
 *                BACKFILL_ENABLED, LOG_PREFIX, LOG_LEVEL, R2_BUCKET_NAME,
 *                PARSE_QUEUE_NAME
 */
'use strict';

// ─── 常量 ──────────────────────────────────────────────────────────────────
const STATE_KEY              = 'backfill-state/progress.json';
const BATCH_PREFIX           = 'processed/';
const RECOVER_MARKER_PREFIX  = '.recover-done-';
const MAX_RANGE_HOURS        = 48;
const WALL_TIME_BUDGET_MS    = 60_000;          // 每次 Cron 总预算 60s
const CLEANUP_BUDGET_MS      = 30_000;          // Phase1 预算，留 30s 给 Phase2
const MAX_RATE               = 100;
const DEFAULT_RATE           = 10;
const LIST_LIMIT             = 1000;
const DELETE_CONCURRENCY     = 50;
const MAX_DAY_PREFIXES       = 5;               // 48h 跨越最多 3 天，留 safety
const LOG_LEVELS             = Object.freeze({ debug: 0, info: 1, warn: 2, error: 3 });

// ─── 主入口 ────────────────────────────────────────────────────────────────
export default {
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runBackfill(env));
  },

  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === '/backfill/status') {
      const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
      if (!obj) {
        return jsonResponse({
          status: 'not_started',
          message: 'No backfill state found yet. Ensure BACKFILL_START_TIME and BACKFILL_END_TIME are set in wrangler_backfill.toml and wait for the next Cron trigger (within 1 minute).',
          config_hint: {
            BACKFILL_START_TIME: env.BACKFILL_START_TIME || '(unset)',
            BACKFILL_END_TIME:   env.BACKFILL_END_TIME   || '(unset)',
            BACKFILL_RATE:       env.BACKFILL_RATE       || String(DEFAULT_RATE),
            BACKFILL_ENABLED:    env.BACKFILL_ENABLED    || '(default: true)',
          }
        });
      }
      try {
        const data = JSON.parse(await obj.text());
        return jsonResponse(data);
      } catch (e) {
        return jsonResponse({ status: 'error', message: `State file corrupted: ${e.message}` }, 500);
      }
    }

    if (url.pathname === '/' || url.pathname === '') {
      return new Response(
        'ctyun-logpush-backfill worker\n' +
        'GET /backfill/status — view backfill progress\n',
        { status: 200, headers: { 'content-type': 'text/plain; charset=utf-8' } }
      );
    }

    return new Response('Not Found', { status: 404 });
  }
};

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { 'content-type': 'application/json; charset=utf-8' }
  });
}

// ─── 主流程 ────────────────────────────────────────────────────────────────
async function runBackfill(env) {
  const startedAt = Date.now();
  try {
    // 1. 解析并校验配置
    const config = parseConfig(env);
    if (!config.valid) {
      log(env, 'error', config.error);
      return;
    }

    // 2. 暂停开关
    if (env.BACKFILL_ENABLED === 'false') {
      log(env, 'info', 'Paused by BACKFILL_ENABLED=false');
      return;
    }

    // 3. 加载 state（检测 config 变更自动 re-init）
    const state = await loadState(env, config);

    // 4. 已完成则空操作
    if (state.status === 'done') {
      log(env, 'info', `Backfill already done. cleaned=${state.cleanup.total}, enqueued=${state.enqueued_count}, completed_at=${state.completed_at}. To re-run: change BACKFILL_START_TIME/END_TIME in wrangler_backfill.toml and redeploy, OR manually delete R2 object '${STATE_KEY}'.`);
      return;
    }

    // 5. 执行当前 phase（cleanup 完成后若有剩余预算，顺势进入 enqueue）
    let changed = false;
    if (state.phase === 'cleanup') {
      changed = (await runCleanup(env, state, config, startedAt)) || changed;
    }
    if (state.phase === 'enqueue' && Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
      changed = (await runEnqueue(env, state, config, startedAt)) || changed;
    }

    // 6. 保存 state（失败仅 warn，下次 cron 从旧 cursor 继续，幂等保护）
    if (changed) {
      await saveState(env, state).catch((e) => {
        log(env, 'warn', `Failed to save state (will retry next cron, idempotent): ${e.message}`);
      });
    }
  } catch (e) {
    log(env, 'error', `Backfill cron crashed: ${e.message}\n${e.stack}`);
  }
}

// ─── 配置解析 ─────────────────────────────────────────────────────────────
function parseConfig(env) {
  const start = (env.BACKFILL_START_TIME || '').trim();
  const end   = (env.BACKFILL_END_TIME   || '').trim();

  if (!start || !end) {
    return {
      valid: false,
      error: 'BACKFILL_START_TIME and BACKFILL_END_TIME must be set. Format: ISO 8601, e.g. "2026-04-22T14:00:00Z" (UTC) or "2026-04-22T22:00:00+08:00" (Beijing time).'
    };
  }
  const startMs = new Date(start).getTime();
  const endMs   = new Date(end).getTime();
  if (isNaN(startMs) || isNaN(endMs)) {
    return { valid: false, error: `Invalid time format. START="${start}" END="${end}". Expected ISO 8601.` };
  }
  if (startMs >= endMs) {
    return { valid: false, error: `START (${start}) must be earlier than END (${end}).` };
  }
  // 关键安全校验：END 不能是未来时间
  // 否则 cleanup 阶段会把 [START, 未来] 范围内正在被生产 worker 写入的实时 processed/
  // 文件误删，导致正在进行的真实业务数据丢失
  const now = Date.now();
  if (endMs > now) {
    const futureMin = ((endMs - now) / 60000).toFixed(1);
    return {
      valid: false,
      error: `END (${end}) must not be in the future (${futureMin}min ahead of now). Backfill is for historical data only — a future END would cause cleanup to delete live processed/ files being written by the production worker.`
    };
  }
  const spanHours = (endMs - startMs) / 3600000;
  if (spanHours > MAX_RANGE_HOURS) {
    return {
      valid: false,
      error: `Range span ${spanHours.toFixed(1)}h exceeds max ${MAX_RANGE_HOURS}h. Intentional? Edit MAX_RANGE_HOURS in index_backfill.js.`
    };
  }

  const rateRaw = parseInt(env.BACKFILL_RATE || String(DEFAULT_RATE), 10);
  const rate    = Math.min(MAX_RATE, Math.max(1, isNaN(rateRaw) ? DEFAULT_RATE : rateRaw));

  return {
    valid:      true,
    start, end, startMs, endMs, rate,
    bucketName: env.R2_BUCKET_NAME || 'cdn-logs-raw',
    logPrefix:  env.LOG_PREFIX     || 'logs/'
  };
}

// ─── State 持久化 ─────────────────────────────────────────────────────────
async function loadState(env, config) {
  const obj = await env.RAW_BUCKET.get(STATE_KEY).catch(() => null);
  if (obj) {
    try {
      const state = JSON.parse(await obj.text());
      // 检测 config 是否变化（客户改了 env 想重跑）
      if (state.config?.start === config.start && state.config?.end === config.end) {
        return state;
      }
      log(env, 'info', `Config changed (was ${state.config?.start}→${state.config?.end}, now ${config.start}→${config.end}). Re-initializing state.`);
    } catch (e) {
      log(env, 'warn', `State file corrupted, re-initializing: ${e.message}`);
    }
  }
  // 初始化
  return {
    config:           { start: config.start, end: config.end, rate: config.rate },
    phase:            'cleanup',
    status:           'running',
    started_at:       new Date().toISOString(),
    cleanup: {
      start_after:    null,
      batches_deleted: 0,
      dones_deleted:   0,
      total:           0,
      completed_at:    null
    },
    enqueue_progress: {},          // { [prefix]: { start_after, done, enqueued } }
    enqueued_count:   0,
    last_cron_at:     null,
    completed_at:     null
  };
}

async function saveState(env, state) {
  state.last_cron_at = new Date().toISOString();
  await env.RAW_BUCKET.put(STATE_KEY, JSON.stringify(state, null, 2), {
    httpMetadata: { contentType: 'application/json; charset=utf-8' }
  });
}

// ─── Phase 1: 清理 processed/ 下时间范围内的脏数据 ─────────────────────────
// 用 startAfter 而非 cursor：基于 key 字典序，可重入、可恢复，中途 break 不会丢数据
async function runCleanup(env, state, config, startedAt) {
  let scanned = 0;
  let deletedBatches = 0;
  let deletedDones = 0;
  let startAfter = state.cleanup.start_after || undefined;
  let done = false;

  log(env, 'info', `[Cleanup] Scanning ${BATCH_PREFIX} for files in [${config.start}, ${config.end}], startAfter=${startAfter || '(none)'}`);

  while (Date.now() - startedAt < CLEANUP_BUDGET_MS) {
    const list = await env.RAW_BUCKET.list({
      prefix:     BATCH_PREFIX,
      startAfter,
      limit:      LIST_LIMIT
    });

    if (list.objects.length === 0) {
      done = true;
      break;
    }
    scanned += list.objects.length;

    const toDelete = [];
    for (const obj of list.objects) {
      const range = extractFileTimeRange(obj.key);
      if (!range) continue;
      // 任何与 [A, B] 有交集的都要清
      if (range.endMs < config.startMs || range.startMs > config.endMs) continue;
      toDelete.push({ key: obj.key, isDone: obj.key.endsWith('.done') });
    }

    // 并发 delete 当前页的所有匹配文件
    // 关键：必须全部删完才能推进 startAfter；否则中途超时后未删的文件漏掉
    let allDeletedThisPage = true;
    for (let i = 0; i < toDelete.length; i += DELETE_CONCURRENCY) {
      if (Date.now() - startedAt >= CLEANUP_BUDGET_MS) {
        allDeletedThisPage = false;
        break;
      }
      const chunk = toDelete.slice(i, i + DELETE_CONCURRENCY);
      const results = await Promise.allSettled(chunk.map(({ key }) => env.RAW_BUCKET.delete(key)));
      for (let j = 0; j < results.length; j++) {
        if (results[j].status === 'fulfilled') {
          if (chunk[j].isDone) deletedDones++;
          else                 deletedBatches++;
        } else {
          log(env, 'warn', `Delete failed for ${chunk[j].key}: ${results[j].reason?.message || results[j].reason}`);
        }
      }
    }

    if (!allDeletedThisPage) {
      // 本页未删完，保留旧 startAfter，下次 cron 重扫本页（delete 幂等，重复删 not-exist 是 no-op）
      log(env, 'debug', `[Cleanup] Budget exhausted mid-page, keeping startAfter for retry`);
      break;
    }

    // 本页全删完，推进 startAfter 到本页最后一个 key
    startAfter = list.objects[list.objects.length - 1].key;
    state.cleanup.start_after = startAfter;

    // R2 list 返回少于 limit 表示已到末尾
    if (list.objects.length < LIST_LIMIT) {
      done = true;
      break;
    }
  }

  state.cleanup.batches_deleted += deletedBatches;
  state.cleanup.dones_deleted   += deletedDones;
  state.cleanup.total            = state.cleanup.batches_deleted + state.cleanup.dones_deleted;

  log(env, 'info', `[Cleanup] scanned=${scanned} this cron, +${deletedBatches} batches / +${deletedDones} dones. total=${state.cleanup.total}. next=${done ? 'done' : 'continue_next_cron'}`);

  if (done) {
    state.phase = 'enqueue';
    state.cleanup.completed_at = new Date().toISOString();
    log(env, 'info', `[Cleanup] Phase 1 complete. Moving to Phase 2 (enqueue).`);
  }

  return true;
}

// ─── Phase 2: 按日期 prefix 扫描 logs/，rate-limited 入队 parse-queue ────────
// 用 startAfter 而非 cursor：中途 break（rate 满）不丢数据，下次 cron 精确续扫
async function runEnqueue(env, state, config, startedAt) {
  const prefixes = getR2PrefixesByDay(config.startMs, config.endMs, config.logPrefix);

  // 初始化每个 prefix 的 progress
  for (const p of prefixes) {
    if (!state.enqueue_progress[p]) {
      state.enqueue_progress[p] = { start_after: null, done: false, enqueued: 0 };
    }
  }

  let enqueuedThisCron = 0;

  for (const prefix of prefixes) {
    if (enqueuedThisCron >= config.rate) break;
    if (Date.now() - startedAt >= WALL_TIME_BUDGET_MS) break;

    const prog = state.enqueue_progress[prefix];
    if (prog.done) continue;

    while (enqueuedThisCron < config.rate && Date.now() - startedAt < WALL_TIME_BUDGET_MS) {
      const list = await env.RAW_BUCKET.list({
        prefix,
        startAfter: prog.start_after || undefined,
        limit:      LIST_LIMIT
      });

      if (list.objects.length === 0) {
        prog.done = true;
        break;
      }

      // 关键：startAfter 只在一个 key 处理完成后才推进，避免 rate 中途 break 导致漏文件
      let pageExhausted = true;
      for (const obj of list.objects) {
        if (enqueuedThisCron >= config.rate) {
          pageExhausted = false;
          break;
        }
        const key = obj.key;

        // 推进 startAfter 到当前 key（不论是否入队）
        // 这样过滤掉的/不在时间范围的 key 下次也不会重复扫
        prog.start_after = key;

        // 安全跳过：prefix 已经限定了，但仍防御性过滤
        if (key.startsWith(BATCH_PREFIX))          continue;
        if (key.startsWith('backfill-state/'))     continue;
        if (key.startsWith(RECOVER_MARKER_PREFIX)) continue;

        const range = extractFileTimeRange(key);
        if (!range) continue;
        // 时间范围交集判断
        if (range.endMs < config.startMs || range.startMs > config.endMs) continue;

        try {
          // 格式与 R2 Event Notification 原生消息一致，Parser 通过 msg.body.object.key 读取
          await env.PARSE_QUEUE.send({
            bucket: config.bucketName,
            object: { key }
          });
          enqueuedThisCron++;
          prog.enqueued++;
        } catch (e) {
          // Queue send 失败：throw 出去让整个 cron 失败，state 不 save
          // 下次 cron 从上次成功保存的 state 重新开始（start_after 会"回到"上次保存的位置）
          // 已入队的 key 被 Parser 的 .done 机制保护，下游幂等
          log(env, 'error', `Enqueue failed for ${key}: ${e.message}. Cron will retry next minute.`);
          throw e;
        }
      }

      // 如果这一页被 rate 限制打断，保留当前 startAfter（已指向最后处理的 key），下次继续
      if (!pageExhausted) break;

      // 一页扫完（objects 少于 limit 表示末尾）
      if (list.objects.length < LIST_LIMIT) {
        prog.done = true;
        break;
      }
      // 否则 while 循环继续，startAfter 已推进到本页最后一个 key
    }
  }

  state.enqueued_count += enqueuedThisCron;

  const allDone      = prefixes.every(p => state.enqueue_progress[p].done);
  const prefixesDone = prefixes.filter(p => state.enqueue_progress[p].done).length;
  log(env, 'info', `[Enqueue] +${enqueuedThisCron} files this cron. total=${state.enqueued_count}. prefixes_done=${prefixesDone}/${prefixes.length}`);

  if (allDone) {
    state.phase        = 'done';
    state.status       = 'done';
    state.completed_at = new Date().toISOString();
    const durMin = Math.round((Date.parse(state.completed_at) - Date.parse(state.started_at)) / 60000);
    log(env, 'info', `🎉 Backfill ENQUEUE COMPLETE! cleaned=${state.cleanup.total}, enqueued=${state.enqueued_count}, cron_duration=${durMin}min. Production Parser/Sender will continue processing enqueued files asynchronously. Monitor send-queue backlog for actual delivery completion.`);
  }

  return true;
}

// ─── 工具函数 ──────────────────────────────────────────────────────────────

// 从 key 中提取 Logpush 时间戳（支持 raw 和 processed 两种 key 形式）
// raw:       logs/20260422/20260422T140000Z_20260422T140500Z_abc.log.gz
// processed: processed/logs_20260422_20260422T140000Z_20260422T140500Z_abc_log_gz-0.txt[.done]
// 因为 safeKey 仅替换 [^a-zA-Z0-9_-] 为 _，时间戳 YYYYMMDDTHHMMSSZ 完整保留
function extractFileTimeRange(key) {
  // 优先匹配 start_end 双时间戳格式
  const m = key.match(/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z[_-](\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/);
  if (m) {
    const s = Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4],  +m[5],  +m[6]);
    const e = Date.UTC(+m[7], +m[8] - 1, +m[9], +m[10], +m[11], +m[12]);
    if (!isNaN(s) && !isNaN(e)) return { startMs: s, endMs: e };
  }
  // 回退：单个时间戳（部分自定义 Logpush 命名）
  const m2 = key.match(/(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z/);
  if (m2) {
    const t = Date.UTC(+m2[1], +m2[2] - 1, +m2[3], +m2[4], +m2[5], +m2[6]);
    if (!isNaN(t)) return { startMs: t, endMs: t };
  }
  return null;
}

// 根据时间范围生成日期 prefix 列表（限定 R2 list 扫描范围，UTC 日期边界）
function getR2PrefixesByDay(startMs, endMs, basePrefix) {
  const prefixes = [];
  const d = new Date(startMs);
  d.setUTCHours(0, 0, 0, 0);
  const endDay = new Date(endMs);
  endDay.setUTCHours(0, 0, 0, 0);
  let iter = 0;
  while (d.getTime() <= endDay.getTime() && iter++ < MAX_DAY_PREFIXES) {
    const yyyy = d.getUTCFullYear();
    const mm   = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd   = String(d.getUTCDate()).padStart(2, '0');
    prefixes.push(`${basePrefix}${yyyy}${mm}${dd}/`);
    d.setUTCDate(d.getUTCDate() + 1);
  }
  return prefixes;
}

function log(env, level, msg) {
  if ((LOG_LEVELS[level] ?? 1) >= (LOG_LEVELS[env?.LOG_LEVEL] ?? 1)) {
    const fn = level === 'error' ? console.error : level === 'warn' ? console.warn : console.log;
    fn(`[BACKFILL][${level.toUpperCase()}] ${new Date().toISOString()} ${msg}`);
  }
}
