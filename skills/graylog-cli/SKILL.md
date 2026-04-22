---
name: graylog-cli
description: Query Graylog from the terminal. Use when you need to search logs, fetch errors, run aggregations, inspect streams, check system health, or authenticate against a Graylog instance.
---

# Graylog CLI

A command-line interface for Graylog. Runtime command success output is JSON on stdout, and runtime command failures are JSON on stderr. Clap help/version output remains plain text. No interactive prompts.

## When to Use

- Searching Graylog messages for a query string or pattern
- Fetching recent errors from a Graylog instance
- Running aggregation queries (terms, date histogram, cardinality, stats)
- Counting messages by log level
- Listing, finding, or searching within Graylog streams
- Checking Graylog system info or reachability
- Persisting Graylog credentials for non-interactive use

## Prerequisites

- A running Graylog instance with an access token
- The `graylog-cli` binary on PATH (or invoke as `./target/debug/graylog-cli` / `./target/release/graylog-cli`)
- No TTY required. All commands are non-interactive

## One-Time Auth

Store credentials before running any query. The command writes a TOML config and exits. Re-running overwrites without prompting.

```bash
graylog-cli auth -u <URL> -t <TOKEN>
```

- `<URL>` is the Graylog base URL, e.g. `https://graylog.example.com`
- `<TOKEN>` is a Graylog access token. Use an environment variable in practice: `$GRAYLOG_TOKEN`

Config is stored at:

| Condition | Path |
|-----------|------|
| `XDG_CONFIG_HOME` is set | `$XDG_CONFIG_HOME/graylog-cli/config.toml` |
| Default | `$HOME/.config/graylog-cli/config.toml` |

Directory permissions are `0700`, file permissions are `0600`.

## Command Map

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
| `--offset` | non-negative integer | |
| `--sort` | field name | |
| `--sort-direction` | `asc`, `desc` | |
| `--stream-id` | repeatable | Scope search to specific streams |

### errors

Fetch recent error messages.

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

`streams search` accepts `--time-range`/`--from`/`--to`, `--field` (repeatable), and `--limit` (1-100).
`streams last-event` accepts `--time-range`/`--from`/`--to`.

### system

Inspect Graylog system details.

```bash
graylog-cli system info
```

### ping

Check that Graylog is reachable and credentials are valid.

```bash
graylog-cli ping
```

## Common Workflows

### Find recent errors in a service

```bash
graylog-cli errors --time-range 30m --limit 20
```

### Search for a specific message pattern

```bash
graylog-cli search "connection refused" --time-range 1h --field message --field source
```

### Count log levels over the last hour

```bash
graylog-cli count-by-level --time-range 1h
```

### Top source IPs from the past day

```bash
graylog-cli aggregate "*" --aggregation-type terms --field source --size 10 --time-range 1d
```

### Find a stream and search within it

```bash
graylog-cli streams find "production"
graylog-cli streams search <STREAM_ID> "timeout" --time-range 15m
```

### Verify connectivity

```bash
graylog-cli ping
```

## Safety Rules

- **Never log or print the token.** Use `$GRAYLOG_TOKEN` or a secret manager. Do not paste real tokens into shell history or CI config
- **Runtime command output is JSON.** Pipe successful command output through `jq` for filtering. Built-in `--help` and `--version` output remain plain text from clap
- **Errors go to stderr.** Successful output goes to stdout. Check exit codes:
  - `0` success
  - `1` internal error
  - `2` validation or config error
  - `3` auth error (invalid or expired token)
  - `4` not found or unsupported endpoint
  - `5` network or transport error
- **Auth overwrites silently.** Running `graylog-cli auth` again replaces the stored config without confirmation
- **No interactive prompts.** Every command runs to completion or exits with a non-zero code. Safe for CI

## Troubleshooting

**"not configured" error**

Run `graylog-cli auth -u <URL> -t <TOKEN>` first. Check that the config file exists at the expected path (see [One-Time Auth](#one-time-auth)).

**Exit code 3 (auth error)**

The stored token is invalid or expired. Generate a new token in Graylog and re-run `graylog-cli auth`.

**Exit code 5 (network error)**

Graylog is unreachable. Check the URL, network connectivity, and any proxy settings. `graylog-cli ping` is the quickest way to verify reachability.

**Empty results from search or aggregate**

The timerange may not cover the period you expect. Try a wider range with `--time-range 1d`. Verify the query syntax matches Graylog's Lucene-based search language.

**"interval is only supported when aggregation-type date_histogram is selected"**

Remove `--interval` from non-histogram aggregations, or switch to `--aggregation-type date_histogram`.
