---
name: graylog-cli
description: Query and analyze Graylog logs from the terminal. Use when debugging issues, investigating errors, analyzing system behavior, or monitoring log patterns in Graylog. Supports search, aggregation, stream inspection, and system health checks.
argument-hint: <command> [args]
---

# Graylog CLI

A command-line interface for Graylog. Runtime command success output is JSON on stdout, and runtime command failures are JSON on stderr. Clap help/version output remains plain text. No interactive prompts.

## When to Use

- **Debugging**: Investigate errors, trace request flows, find root causes
- **Monitoring**: Check error rates, count by log level, verify system health
- **Investigation**: Search for specific patterns, aggregate trends, explore streams
- **CI/automation**: Non-interactive JSON output, exit codes for scripting

## Prerequisites

- A running Graylog instance with an access token
- The `graylog-cli` binary on PATH
- No TTY required. All commands are non-interactive

## One-Time Auth

Store credentials before running any query. Re-running overwrites without prompting.

```bash
graylog-cli auth --url <URL> --token <TOKEN>
```

- `<URL>` is the Graylog base URL, e.g. `https://graylog.example.com`
- `<TOKEN>` is a Graylog access token. Use an environment variable in practice: `$GRAYLOG_TOKEN`

Config is stored at:

| Condition | Path |
|-----------|------|
| `XDG_CONFIG_HOME` is set | `$XDG_CONFIG_HOME/graylog-cli/config.toml` |
| Unix default | `$HOME/.config/graylog-cli/config.toml` |
| Windows default | `%APPDATA%\graylog-cli\config.toml` |

On Unix, directory permissions are `0700` and file permissions are `0600`. On Windows, NTFS ACLs inherit from the parent directory — no explicit permission hardening is applied.

## Graylog Query Language

The `search`, `aggregate`, and `streams search` commands accept Graylog's Lucene-based query syntax. Understanding the query language is essential for effective use.

### Log Levels (Syslog Severity)

Graylog stores `level` as a **numeric** field (0–7):

| Level | Severity | Meaning |
|:-----:|----------|---------|
| 0 | Emergency | System unusable |
| 1 | Alert | Immediate action needed |
| 2 | Critical | Critical condition |
| 3 | Error | Error condition |
| 4 | Warning | Warning condition |
| 5 | Notice | Normal but notable |
| 6 | Informational | Informational |
| 7 | Debug | Debug messages |

### Query Construction Rules

1. **Keywords**: Use directly — `checkout`, `timeout`, `payment`
2. **Level filtering**: Use `level:<=N` to include all levels from 0 (most severe) through N. Example: `level:<=3` matches Emergency, Alert, Critical, and Error
3. **Field-specific**: Use `field:value` syntax — `source:api-gateway`, `facility:cron`
4. **Combining**: Use `AND`, `OR`, `NOT` — `"connection refused" AND level:<=3`
5. **Wildcards**: `message:*timeout*` for substring matches
6. **Grouping**: Parentheses for precedence — `(source:svc-a OR source:svc-b) AND level:<=4`

### Common Query Patterns

```bash
# Errors and above for a specific service
"source:payment-service AND level:<=3"

# Specific keyword with warning level and below
"checkout AND level:<=4"

# Exclude debug noise
"* AND NOT level:7"

# Match any message (wildcard)
"*"
```

## Command Reference

### search

Search Graylog messages.

```bash
graylog-cli search <QUERY> [--time-range 15m] [--field message] [--field source] \
  [--limit 50] [--offset 0] [--sort timestamp] [--sort-direction desc] [--stream-id <ID>]
```

| Flag | Values | Notes |
|------|--------|-------|
| `--time-range` | `Ns`, `Nm`, `Nh`, `Nd`, `Nw` | Relative range. Mutually exclusive with `--from`/`--to` |
| `--from` / `--to` | ISO 8601 timestamps | Absolute range. Both required together |
| `--field` | repeatable | Restrict returned fields |
| `--limit` | 1-1000 | |
| `--offset` | non-negative integer | Pagination offset |
| `--sort` | field name | Default: `timestamp` |
| `--sort-direction` | `asc`, `desc` | Default: `desc` |
| `--stream-id` | repeatable | Scope search to specific streams |

### errors

Fetch recent error-level messages (level 0–3: Emergency, Alert, Critical, Error). Uses the query `level:<=3`.

```bash
graylog-cli errors [--time-range 1h] [--limit 100]
```

Accepts `--time-range` / `--from`/`--to` and `--limit` (1-1000).

### aggregate

Run an aggregation query.

```bash
graylog-cli aggregate <QUERY> --aggregation-type <TYPE> --field <FIELD> \
  [--size 10] [--interval 1h] [--time-range 1d]
```

| `--aggregation-type` | Notes |
|----------------------|-------|
| `terms` | Top values for a field |
| `date_histogram` | Requires `--interval` |
| `cardinality` | Distinct count |
| `stats` | Stats over a numeric field |
| `min`, `max`, `avg`, `sum` | Single-metric aggregations |

`--size` accepts 1-100. `--interval` is required for `date_histogram` and forbidden for all other types.

### count-by-level

Count messages grouped by log level. Equivalent to a `terms` aggregation on the `level` field.

```bash
graylog-cli count-by-level --time-range 1h
```

Accepts `--time-range` / `--from`/`--to`.

### streams

Work with Graylog streams.

```bash
graylog-cli streams list
graylog-cli streams show <STREAM_ID>
graylog-cli streams find <NAME>
graylog-cli streams search <STREAM_ID> <QUERY> [--time-range 15m] [--field message] [--limit 50]
graylog-cli streams last-event <STREAM_ID> --time-range 1h
```

- `streams find` searches by name (case-insensitive substring match)
- `streams search` accepts `--time-range`/`--from`/`--to`, `--field` (repeatable), and `--limit` (1-100)
- `streams last-event` accepts `--time-range`/`--from`/`--to`

### system

Inspect Graylog system details.

```bash
graylog-cli system info
```

### fields

List all indexed fields available for querying and filtering.

```bash
graylog-cli fields
```

Returns every field name that Graylog has indexed across all messages. Use this to discover what fields you can pass to `--field`, use in queries (`field:value`), or aggregate on. No flags required.

Key fields for debugging:
- `checkoutCorrelationId` — groups all events for a single checkout order
- `correlationId` — individual request/trace ID
- `merchant` — merchant identifier (e.g. `ppg`)
- `environment` — `Production`, `Stage`, etc.
- `facility` — service/logger category (e.g. `checkout-order`)
- `level` — numeric log level (0–7)
- `message` — log message text
- `Request_Body` / `Response_Body` — full HTTP request/response payloads

### trace

Trace all events matching a query, grouped into a structured timeline with noise collapsed and key events highlighted. Accepts the same Graylog query syntax as `search`.

```bash
graylog-cli trace <QUERY> [--group-by correlationId] [--time-range 2h]
```

| Flag | Values | Notes |
|------|--------|-------|
| `--group-by` | Any indexed field name | Default: `correlationId`. Controls how events are grouped into timeline segments |
| `--time-range` | `Ns`, `Nm`, `Nh`, `Nd`, `Nw` | Default: `1h`. Mutually exclusive with `--from`/`--to` |
| `--from` / `--to` | ISO 8601 timestamps | Absolute range. Both required together |

Examples:
```bash
# Trace a checkout order
graylog-cli trace "checkoutCorrelationId:omggXmLy" --time-range 2h

# Trace a single request
graylog-cli trace "correlationId:fe165125-90da-4fbb-bf53-33d6dac6c038" --time-range 2h

# Trace by basket
graylog-cli trace "BasketId:140597614" --time-range 4h

# Trace all errors for a merchant, grouped by order
graylog-cli trace "merchant:ppg AND level:<=3" --group-by checkoutCorrelationId --time-range 1h

# Trace errors for a specific service
graylog-cli trace "source:qliro-adapter AND level:<=3" --time-range 30m
```

The output groups events by `correlationId` (individual request traces) and categorizes each event:

| Event type | What it captures |
|-----------|-----------------|
| `error` | Log entries with level ≤ 3 (full message shown) |
| `warning` | Log entries with level 4 (full message shown) |
| `external_call` | HTTP requests to external APIs (method + target URL) |
| `external_call_response` | HTTP responses (status code + duration in ms) |
| `callback` | Hook/callback invocations (target adapter + path) |
| `state_change` | Object comparison diffs (field changes detected) |
| `db_op` | Cosmos DB reads/writes (collapsed to count) |
| `internal` | Request started/processed logs (collapsed to count) |

Each trace group includes the `correlation_id`, a `trigger` (first request path), `duration_ms`, and the categorized `events` array. A `summary` section provides totals for errors, external calls, and services involved.

### ping

Check that Graylog is reachable and credentials are valid.

```bash
graylog-cli ping
```

## Investigation Workflows

When investigating an issue, follow these patterns. Output is JSON — pipe through `jq` for filtering.

### "What's breaking right now?"

Quick snapshot of current errors:

```bash
# Recent errors with full context
graylog-cli errors --time-range 30m --limit 20 | jq .

# Error distribution by level
graylog-cli count-by-level --time-range 1h | jq .

# Error distribution by source
graylog-cli aggregate "level:<=3" --aggregation-type terms --field source --size 20 --time-range 1h | jq .

# Error distribution by merchant
graylog-cli aggregate "level:<=3" --aggregation-type terms --field merchant --size 20 --time-range 1h | jq .
```

Then trace specific failing orders:

```bash
# Get affected order IDs from errors
graylog-cli search "level:<=3" --field checkoutCorrelationId --limit 50 --time-range 1h | jq '[.messages[]."field: checkoutCorrelationId" | select(. != null)] | unique'

# Trace one of them
graylog-cli trace omggXmLy --time-range 2h | jq .
```

### "What fields can I query?"

```bash
# List all indexed fields
graylog-cli fields | jq .

# Find correlation-related fields
graylog-cli fields | jq '.fields[] | select(test("correlation|checkout|merchant|environment"))'
```

### "Trace a specific order through the system"

```bash
# Full timeline for a checkout order
graylog-cli trace "checkoutCorrelationId:omggXmLy" --time-range 2h | jq .

# Trace a single request
graylog-cli trace "correlationId:fe165125-90da-4fbb-bf53-33d6dac6c038" --time-range 2h | jq .

# Just the errors and the summary
graylog-cli trace "checkoutCorrelationId:omggXmLy" --time-range 2h | jq '{total_events, errors: [.trace_groups[].events[] | select(.type == "error")], summary}'

# Find all correlation IDs for an order
graylog-cli trace "checkoutCorrelationId:omggXmLy" | jq '[.trace_groups[].correlation_id]'

# Show only external calls (API interactions)
graylog-cli trace "checkoutCorrelationId:omggXmLy" | jq '[.trace_groups[].events[] | select(.type == "external_call" or .type == "external_call_response")]'
```

### "Show me all logs for X"

Search for a keyword, order ID, request ID, or error message:

```bash
# Simple keyword search
graylog-cli search "checkout" --time-range 1h --limit 50 | jq .

# Scoped to a stream
graylog-cli search "timeout" --stream-id <STREAM_ID> --time-range 15m | jq .

# Specific fields only (smaller output)
graylog-cli search "payment" --field message --field source --field timestamp --limit 50 | jq .
```

### "Is service X healthy?"

Check error rates and patterns for a specific service:

```bash
# All errors from a service
graylog-cli search "source:api-gateway AND level:<=3" --time-range 1h | jq .

# Error count breakdown
graylog-cli aggregate "source:api-gateway AND level:<=3" --aggregation-type terms --field level --time-range 1h | jq .

# Error trend over time
graylog-cli aggregate "source:api-gateway AND level:<=3" --aggregation-type date_histogram --field timestamp --interval 1h --time-range 1d | jq .
```

### "What are the top N values for field X?"

```bash
# Top sources
graylog-cli aggregate "*" --aggregation-type terms --field source --size 10 --time-range 1d | jq .

# Top error messages
graylog-cli aggregate "level:<=3" --aggregation-type terms --field message --size 20 --time-range 1h | jq .

# Unique values (cardinality)
graylog-cli aggregate "*" --aggregation-type cardinality --field source --time-range 1h | jq .
```

### "Find a stream and search within it"

```bash
# Find by name
graylog-cli streams find "production" | jq .

# Search within the found stream
graylog-cli streams search <STREAM_ID> "error" --time-range 15m --limit 50 | jq .

# Get the last event in a stream
graylog-cli streams last-event <STREAM_ID> --time-range 1h | jq .
```

### "Verify connectivity and auth"

```bash
graylog-cli ping | jq .
graylog-cli system info | jq .
```

## Interpreting Output

All runtime output is JSON. Key fields:

**Search results** — `messages` array with `returned` count and `query` echo:
```json
{
  "ok": true,
  "command": "search",
  "query": "checkout AND level:<=3",
  "returned": 42,
  "messages": [...],
  "metadata": { "total_results": 420 }
}
```

**Aggregation results** — `rows` array:
```json
{
  "ok": true,
  "command": "aggregate",
  "aggregation_type": "terms",
  "rows": [
    { "source": "api-gateway", "count()": 150 },
    { "source": "payment-service", "count()": 23 }
  ],
  "metadata": {}
}
```

**Error responses** — always include `ok: false`, `code`, and `message`:
```json
{
  "ok": false,
  "code": "auth_error",
  "message": "Graylog rejected the supplied credentials"
}
```

## Exit Codes

| Code | Meaning |
|:----:|---------|
| 0 | Success |
| 1 | Internal error |
| 2 | Validation or config error |
| 3 | Auth error (invalid or expired token) |
| 4 | Not found or unsupported endpoint |
| 5 | Network or transport error |

## Safety Rules

- **Never log or print the token.** Use `$GRAYLOG_TOKEN` or a secret manager. Do not paste real tokens into shell history or CI config
- **Runtime command output is JSON.** Pipe through `jq` for filtering. Built-in `--help` and `--version` remain plain text
- **Auth overwrites silently.** Running `graylog-cli auth` again replaces the stored config without confirmation
- **No interactive prompts.** Every command runs to completion or exits with a non-zero code. Safe for CI

## Troubleshooting

**"not configured" error**

Run `graylog-cli auth --url <URL> --token <TOKEN>` first. Check that the config file exists at the expected path (see [One-Time Auth](#one-time-auth)).

**Exit code 3 (auth error)**

The stored token is invalid or expired. Generate a new token in Graylog and re-run `graylog-cli auth`.

**Exit code 5 (network error)**

Graylog is unreachable. Check the URL, network connectivity, and any proxy settings. `graylog-cli ping` is the quickest way to verify reachability.

**Empty results from search or aggregate**

The timerange may not cover the period you expect. Try a wider range with `--time-range 1d`. Verify the query syntax — remember that `level` is numeric (0–7), not text.

**"interval is only supported when aggregation-type date_histogram is selected"**

Remove `--interval` from non-histogram aggregations, or switch to `--aggregation-type date_histogram`.

**Search returns no matches but logs exist**

The query may be using text-based level names. Use numeric operators: `level:<=3` (not `level:ERROR`). See [Log Levels](#log-levels-syslog-severity).
