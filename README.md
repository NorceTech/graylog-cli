# graylog-cli

Command-line interface for Graylog.

## Install

Download the latest binary for your platform from [Releases](https://github.com/NorceTech/graylog-cli/releases/latest).

### macOS (Apple Silicon)

```sh
curl -sL https://github.com/NorceTech/graylog-cli/releases/latest/download/graylog-cli-macos-aarch64 -o /usr/local/bin/graylog-cli
chmod +x /usr/local/bin/graylog-cli
```

### Linux (x86_64)

```sh
curl -sL https://github.com/NorceTech/graylog-cli/releases/latest/download/graylog-cli-linux-x86_64 -o /usr/local/bin/graylog-cli
chmod +x /usr/local/bin/graylog-cli
```

### Windows (x86_64)

```powershell
$installDir = Join-Path $env:LOCALAPPDATA 'Programs\graylog-cli'
New-Item -ItemType Directory -Path $installDir -Force | Out-Null
Invoke-WebRequest `
    -Uri 'https://github.com/NorceTech/graylog-cli/releases/latest/download/graylog-cli-windows-x86_64.exe' `
    -OutFile (Join-Path $installDir 'graylog-cli.exe') `
    -UseBasicParsing

$userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (-not (($userPath -split ';') -contains $installDir)) {
    [Environment]::SetEnvironmentVariable(
        'Path',
        ($userPath.TrimEnd(';') + ';' + $installDir),
        'User'
    )
}
```

## Usage

### Authenticate

```sh
graylog-cli auth --url https://graylog.example.com --token <your-access-token>
```

The token can also be supplied via the `GRAYLOG_TOKEN` environment variable. Credentials are persisted locally for subsequent commands.

```sh
graylog-cli --help
graylog-cli <command> --help
```

### Time ranges

Most commands accept a time range. Three forms are supported:

| Flag                      | Description                    | Example                                                 |
| ------------------------- | ------------------------------ | ------------------------------------------------------- |
| `--since <duration>`      | Relative to now, shorthand     | `--since 1h`, `--since 30m`, `--since 7d`               |
| `--time-range <duration>` | Relative to now, Graylog-style | `--time-range 1h`                                       |
| `--from <ts> --to <ts>`   | Absolute range (RFC 3339)      | `--from 2024-01-01T00:00:00Z --to 2024-01-02T00:00:00Z` |

`--since` and `--time-range` are mutually exclusive with `--from`/`--to`.

### Search

Search Graylog messages using Lucene syntax.

```sh
# Basic search
graylog-cli search "level:ERROR"

# Last hour, specific fields, limit results
graylog-cli search "level:ERROR" --since 1h --field message --field source --limit 50

# Absolute time range, table output
graylog-cli search "http_status:500" \
  --from 2024-01-01T00:00:00Z --to 2024-01-01T06:00:00Z \
  --format table

# Paginate through all results
graylog-cli search "level:WARN" --since 24h --all-pages

# Scope to a specific stream
graylog-cli search "level:ERROR" --since 1h --stream-id <stream-id>

# Group results by a field
graylog-cli search "level:ERROR" --since 1h --group-by source
```

Key flags: `--field` (repeatable), `--limit` (1–1000), `--offset`, `--sort`, `--sort-direction asc|desc`, `--all-pages`, `--all-fields`, `--stream-id` (repeatable), `--format json|table`.

### Aggregate

Run aggregation queries over Graylog messages.

```sh
# Top error sources (terms)
graylog-cli aggregate "level:ERROR" --aggregation-type terms --field source --since 1h

# Message volume over time (date histogram)
graylog-cli aggregate "*" --aggregation-type date_histogram --field timestamp \
  --interval 1h --since 24h

# Distinct values count
graylog-cli aggregate "*" --aggregation-type cardinality --field source --since 1h

# Stats / min / max / avg / sum on a numeric field
graylog-cli aggregate "*" --aggregation-type avg --field response_time_ms --since 1h
```

Aggregation types: `terms`, `date_histogram` (requires `--interval`), `cardinality`, `stats`, `min`, `max`, `avg`, `sum`.

Key flags: `--size` (1–100, for terms), `--interval` (for date_histogram), `--format json|table`.

### Count by level

Quickly tally messages by log level.

```sh
graylog-cli count-by-level --since 1h
graylog-cli count-by-level --since 24h --format table
```

### Streams

```sh
# List all streams
graylog-cli streams list

# Show stream details by ID
graylog-cli streams show <stream-id>

# Find a stream by name
graylog-cli streams find "my-service"

# Search within a stream
graylog-cli streams search <stream-id> "level:ERROR" --since 1h --field message

# Get the last event in a stream
graylog-cli streams last-event <stream-id> --since 24h
```

### System

```sh
# Show Graylog system information
graylog-cli system info
```

### Ping

Check that Graylog is reachable with the configured credentials.

```sh
graylog-cli ping
```

### Fields

List all indexed fields. Results are cached for 5 minutes by default.

```sh
graylog-cli fields
graylog-cli fields --refresh   # bypass cache
```

### Updates

Upgrade to the latest release in place:

```sh
graylog-cli upgrade
```

Every successful command also fires a detached background check (throttled to once per 24h). If a newer release exists, the new binary is downloaded to the config directory and swapped in on the next start.

Opt out persistently by adding this to your `config.toml`:

```toml
[updater]
disable_auto_update = true
```

Or opt out for a single invocation with `GRAYLOG_CLI_AUTO_UPDATE=0` (the env var, when set, takes precedence over the config setting).

## Configuration

Credentials are written to a `config.toml` file in the platform config directory on first `auth`. The file stores one or more named profiles and marks one of them active:

```toml
active_profile = "prod"

[profiles.prod]
url = "https://graylog.example.com"
token = "your-access-token"
timeout_seconds = 60       # default: 60
verify_tls = true          # default: true
fields_cache_ttl_seconds = 300  # default: 300

[profiles.staging]
url = "https://staging.graylog.example.com"
token = "staging-access-token"

[updater]
disable_auto_update = false  # default: false
```

### Profiles

A profile is a complete set of Graylog credentials. `graylog-cli auth` writes the profile selected with the global `--profile` flag (default: `default`) and makes it the active profile:

```sh
graylog-cli auth --url https://graylog.example.com --token TOKEN          # writes profile "default"
graylog-cli --profile staging auth --url https://staging.example.com --token TOKEN
```

Every command runs against the active profile. Use `--profile` (or the `GRAYLOG_PROFILE` environment variable) to target another profile for a single invocation without switching:

```sh
graylog-cli --profile staging search 'level:ERROR'
GRAYLOG_PROFILE=staging graylog-cli ping
```

Profile management commands:

```sh
graylog-cli profiles list            # list profiles (tokens are never shown)
graylog-cli profiles show [name]     # show a profile; defaults to the active one
graylog-cli profiles use staging          # switch the active profile
graylog-cli profiles rename staging prod  # rename a profile
graylog-cli profiles delete staging       # remove a profile
```

Deleting the active profile clears the active selection; the CLI then falls back to the first remaining profile. Deleting the last profile returns the CLI to its not-configured state. Profile names must start with an ASCII letter or digit and may only contain ASCII letters, digits, `.`, `_`, and `-`.

The fields cache is scoped per profile (`fields-<profile>` cache files), so switching profiles never serves stale field lists from another instance.

### Legacy configuration

Configurations written by older versions with a single `[graylog]` table keep working: they are migrated in memory to a `default` profile on load and the file is rewritten in the new format only on the next save (for example the next `auth`). No re-authentication is required.
