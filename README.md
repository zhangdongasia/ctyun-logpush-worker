# ctyun-logpush-worker

Cloudflare Worker that transforms Cloudflare Logpush `http_requests` logs into the CDN partner 145-field format and forwards them to the customer log ingestion endpoint.

## Flow

```text
Cloudflare Logpush -> R2 logs/
  -> R2 Event Notification -> parse-queue
  -> Parser Worker -> R2 processed/*.txt
  -> send-queue -> Sender Worker (serial POST)
  -> gzip + auth_key POST -> customer endpoint
```

Important: configure the R2 Event Notification for raw Logpush objects only: `object-create` with prefix `logs/`. Do not notify on the full bucket, otherwise `processed/` files and marker files may be sent back to `parse-queue`.

## Configure

Before deploying, update `wrangler.toml`:

| Item | Requirement |
|---|---|
| `account_id` | Your Cloudflare account ID |
| `name` | Worker name for this deployment |
| `bucket_name` / `R2_BUCKET_NAME` | Must point to the same R2 bucket |
| Queue names | Keep producer, consumer, `PARSE_QUEUE_NAME`, and `SEND_QUEUE_NAME` in sync |
| `FIELD11_SERVER_IP` | Fixed value for output field #11; empty outputs `-` |
| `BATCH_SIZE` | Lines per POST batch; default `1000`, max `2000` |
| `RAW_LOG_PREFIX` / `RAW_LOG_SUFFIX` | Raw Logpush object allowlist; default `logs/` and `.log.gz` |

Set required secrets:

```bash
wrangler secret put CTYUN_ENDPOINT
wrangler secret put CTYUN_PRIVATE_KEY
wrangler secret put CTYUN_URI_EDGE
```

For GitHub Actions deployment, also set repository secret `CLOUDFLARE_API_TOKEN`.

## Output Format

- Output is 145 fields separated by `\u0001`, per CDN partner log interface v3.0.
- HTTP request body is gzip-compressed before POST.
- `auth_key` is generated as `ts-rand-md5(uri-ts-rand-privateKey)`.
- Field #11 uses `FIELD11_SERVER_IP`.
- Field #21 uses `ResponseHeaders["content-length"]` when Logpush Custom Fields are configured; otherwise `-`.
- Field #45 maps `EdgeColoCode` to CDN node country; unmapped values fall back to `SG`.

## Reliability Notes

- `send-queue` uses `max_concurrency = 1`; Sender POSTs are sequential.
- Delivery is at-least-once; `.done` markers reduce replay after confirmed sends.
- No customer-side changes or custom headers are required.
- Non-raw R2 objects are ignored by the parser; defaults process only `logs/` objects ending in `.log.gz`.

## PUSH_START_TIME

`PUSH_START_TIME` controls when forwarding starts.

| Value | Behavior |
|---|---|
| Empty string | No filtering; process all new logs |
| Future ISO time | Skip files/records before that time until cutover |
| Past ISO time | Cron scans `logs/YYYYMMDD/` and re-enqueues historical files from that time to now |

Recovery is one-time per exact `PUSH_START_TIME` value after successful enqueue using an R2 `.recover-done-*` marker. If recovery enqueue is incomplete, the temporary `.recover-running-*` marker is cleared and the next Cron run retries. Built-in recovery is capped at 62 days; for older or precise `[A, B]` ranges, use the separate backfill worker: [`CFChinaNetwork/ctyun-logpush-backfill`](https://github.com/CFChinaNetwork/ctyun-logpush-backfill).

## Optimized Sender

Default entrypoint: `src/index.js`.

Optional entrypoint: `src/index_optimized.js` streams `R2 -> gzip -> fetch body` directly instead of buffering the compressed body first. This can reduce memory and may improve sender throughput, but it sends the request with chunked transfer encoding and must be validated against the customer endpoint.

Only enable it after confirming the customer endpoint supports HTTP chunked request bodies. If enabling it causes HTTP `400`, `411`, or `415`, roll back by changing `main` in `wrangler.toml` back to `src/index.js`.

## Deploy

Push to `main` triggers GitHub Actions deployment. To validate locally without deploying:

```bash
npx wrangler deploy --dry-run
```

## Documentation

| Language | Guide |
|---|---|
| English | [CF Logpush - Format Transform & Push Guide](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF-Logpush-Format-Transform-and-Push-Guide.html) |
| Chinese | [CF 日志格式转换与自动推送指南](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF%E6%97%A5%E5%BF%97%E6%A0%BC%E5%BC%8F%E8%BD%AC%E6%8D%A2%E4%B8%8E%E8%87%AA%E5%8A%A8%E6%8E%A8%E9%80%81%E6%8C%87%E5%8D%97.html) |
