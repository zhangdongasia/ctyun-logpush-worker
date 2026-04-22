# ctyun-logpush-worker

Cloudflare Worker — transforms and forwards Logpush logs to a CDN partner log ingestion endpoint.

## Pre-Deployment Checklist — Customize for Your Deployment

Before deploying, review and adjust `wrangler.toml` as needed:

| Field | Current Value (example) | What to Change |
|---|---|---|
| `name` | `ctyun-logpush` | Your Worker name (any unique name) |
| `account_id` | `0297df3199a9...` | **Must change** to your own Cloudflare Account ID (`wrangler whoami`) |
| `bucket_name` | `cdn-logs-raw` | Your R2 bucket name |
| `R2_BUCKET_NAME` (in `[vars]`) | `cdn-logs-raw` | Must match `bucket_name` above |
| Queue names (`queue = "..."`) | `parse-queue`, `send-queue` | Your Queue names |
| Queue name vars (`PARSE_QUEUE_NAME`, `SEND_QUEUE_NAME`) | `parse-queue`, `send-queue` | Must match queue names above |

Then set three Worker secrets via `wrangler secret put`:
```bash
wrangler secret put CTYUN_ENDPOINT        # Log ingestion server URL
wrangler secret put CTYUN_PRIVATE_KEY     # Authentication private key
wrangler secret put CTYUN_URI_EDGE        # Target URI path
```

## Architecture

```
CF Edge → Logpush → R2 (cdn-logs-raw)
  → R2 Event Notification → parse-queue
  → Parser Worker (streaming gzip decompress + field mapping)
  → R2 (processed/) → send-queue
  → Sender Worker (gzip + MD5 auth + HTTP POST)
  → Customer log ingestion server
```

## Format

145-field `\u0001`-delimited plaintext format per CDN partner log interface spec v3.0.

## Environment Variables

| Name | Type | Description |
|---|---|---|
| `CLOUDFLARE_API_TOKEN` | Secret (GitHub) | Cloudflare API token for deployment |
| `CTYUN_ENDPOINT` | Secret | Customer log server base URL |
| `CTYUN_PRIVATE_KEY` | Secret | Customer authentication private key |
| `CTYUN_URI_EDGE` | Secret | Customer log POST URI path |
| `BATCH_SIZE` | Var | Log lines per POST request (default: 1000) |
| `LOG_LEVEL` | Var | Logging verbosity: `info` or `debug` |
| `PUSH_START_TIME` | Var | Unified time control (ISO 8601). Empty = disabled. **Future time** → natural wait. **Past time** → auto-recovery: Cron scans R2 and re-enqueues historical files from that time onwards (idempotent). |
| `PARSE_QUEUE_NAME` | Var | Queue name for parse-queue (must match `wrangler.toml`) |
| `SEND_QUEUE_NAME` | Var | Queue name for send-queue (must match `wrangler.toml`) |
| `R2_BUCKET_NAME` | Var | R2 bucket name (must match `wrangler.toml` bucket_name) |

## Deployment

Push to `main` branch triggers automatic deployment via GitHub Actions.

## PUSH_START_TIME — Two Modes in One Variable

Set `PUSH_START_TIME` in `wrangler.toml` to control when log forwarding begins. The Worker automatically detects whether the value is in the future or past:

| Mode | Example | Behavior |
|---|---|---|
| Empty | `""` | No filter, all logs forwarded normally |
| Future time | `"2026-05-01T00:00:00+08:00"` | Parser natural-waits; files before that time are silently skipped |
| **Past time (Recovery)** | `"2026-04-22T15:00:00+08:00"` | **Cron auto-triggers recovery within 1 minute**: scans R2 for all files from that time onwards and re-enqueues them to `parse-queue`. Idempotent via R2 marker file `.recover-done-<timestamp>` |

Recovery use case: customer's original log pipeline failed at 15:00, switched to CF at 16:00. Set `PUSH_START_TIME = "2026-04-22T15:00:00+08:00"`, push — historical 15:00–16:00 logs auto-backfill, new logs continue normally.

## Optional: Enable Content-Length Logging (Field #21)

Field #21 (`sent_http_content_length`) outputs `Content-Length` from `ResponseHeaders` when Logpush Custom Fields are configured, otherwise returns `-`. See the deployment guides in `docs/` for the full API configuration steps.

## Documentation

| Language | File |
|---|---|
| English | [CF Logpush — Format Transform & Push Guide](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF-Logpush-Format-Transform-and-Push-Guide.html) |
| 中文 | [CF 日志格式转换与自动推送指南](https://cfchinanetwork.github.io/ctyun-logpush-worker/docs/CF%E6%97%A5%E5%BF%97%E6%A0%BC%E5%BC%8F%E8%BD%AC%E6%8D%A2%E4%B8%8E%E8%87%AA%E5%8A%A8%E6%8E%A8%E9%80%81%E6%8C%87%E5%8D%97.html) |
