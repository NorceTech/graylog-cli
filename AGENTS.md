# PROJECT KNOWLEDGE BASE

**Generated:** 2026-09-15
**Commit:** bc0f758
**Branch:** main

## OVERVIEW
Rust CLI for Graylog (search/aggregate/streams/system). Hexagonal single crate, tokio + clap + reqwest/rustls.

## STRUCTURE
```
graylog-cli/
├── src/main.rs          # composition root, updater worker
├── src/lib.rs           # pub mods only
├── src/domain/          # config, models, timerange, error
├── src/application/     # service, updater_service, ports/
├── src/infrastructure/  # graylog_client, config_store, updater
├── src/presentation/    # cli (clap), output (json/table)
├── tests/cli_integration.rs
└── benches/
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| CLI commands/flags | `src/presentation/cli.rs` | clap derive, `validate()` + `to_input()` |
| Use-cases | `src/application/service.rs` | `ApplicationService`, 1969 lines |
| HTTP + normalization | `src/infrastructure/graylog_client.rs` | 1847 lines, `X-Requested-By` |
| Config/cache files | `src/infrastructure/config_store.rs` | `~/.config/graylog-cli/config.toml`, 0700 |
| Output/exit codes | `src/presentation/output.rs` | JSON envelope, codes 1-6 |
| Self-update | `src/application/updater_service.rs`, `src/main.rs` | `__self-update-worker`, 24h throttle |
| Release | `.github/workflows/release.yml` | tag `v*` must be on main |

## CODE MAP
| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `ApplicationService` | struct | `application/service.rs` | search/aggregate/auth/ping/streams/system/fields |
| `Config/GraylogConfig/UpdaterConfig` | struct | `domain/config.rs` | TOML config, `SecretString` token |
| `CliError/HttpError/ValidationError` | enum | `domain/error.rs` | layered thiserror + exn |
| `ConfigStore/CacheStore/GraylogGateway/UpdaterGateway` | trait | `application/ports/` | DI seams, glob re-export |
| `FileConfigStore` | struct | `infrastructure/config_store.rs` | atomic write, implements both stores |
| `Cli/Commands` | enum | `presentation/cli.rs` | `auth/search/aggregate/count-by-level/streams/system/ping/fields/upgrade` |
| `print_json/print_table/exit_code_for_cli_error` | fn | `presentation/output.rs` | machine contract |

## CONVENTIONS
- Hexagonal: `application` never does I/O directly, only via `ports` traits; adapters in `infrastructure`.
- Errors: `thiserror` layer enums + `exn::Result`; `main()` returns `()`, single `emit_cli_error` prints JSON to stderr.
- Success = JSON on stdout; `--format table` only exception. No prompts, non-interactive.
- Token via `--token` or `GRAYLOG_TOKEN` env (clap `env`). Auto-update via `GRAYLOG_CLI_AUTO_UPDATE` or `[updater]` TOML.
- Edition 2024 (let-chains), `max_width=100`, treefmt via pre-commit. Conventional Commits.

## ANTI-PATTERNS (THIS PROJECT)
- Don't return `Result` from `main` or add `anyhow`; use `exn` + JSON envelope + semantic exit codes.
- Don't I/O in `ApplicationService`; go through `ports`.
- Don't set `target-cpu=native` in `.cargo/config.toml` (breaks cross builds).
- No `unsafe`, no `#[allow]` in `src/`; clippy `-D warnings` in CI.
- Never serialize `GraylogConfig` to stdout (token is plaintext in TOML serde).

## UNIQUE STYLES
- `lib.rs` is 4 lines; `main.rs` wires `Arc<FileConfigStore>` twice (config + cache).
- Fields cache single global key `"fields"` with TTL 300s.
- Release patches `Cargo.toml` version from tag; `publish=false`, ships binaries only.

## COMMANDS
```bash
nix develop --command cargo test --all --locked
nix develop --command cargo clippy --all-targets --locked -- -D warnings
nix develop --command cargo deny --all-features check
cargo bench  # timerange_parsing, json_normalization
nix build
nix build .#graylog-cli-windows
```

## NOTES
- Two hotspots hold ~52% LOC: `service.rs`, `graylog_client.rs`. `cli.rs` (852) holds full command tree.
- `publish=false`; tags `v*` must point at `main` (verified in release.yml).
- `dirs::config_dir()/graylog-cli/` holds `config.toml` + `<key>.json` caches.
