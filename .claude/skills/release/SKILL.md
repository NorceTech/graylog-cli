---
name: release
description: Create a graylog-cli release by determining the next git tag, synchronizing Cargo.toml and flake.nix versions, verifying the Nix build, tagging, and pushing the release trigger.
disable-model-invocation: true
argument-hint: '[patch|minor|major|<version>]'
allowed-tools: Bash, Read, Edit, Write, Grep, Glob
---

# Release

Use this skill to prepare and publish a `graylog-cli` release. Releases are triggered by pushing a `v*` git tag; the GitHub Actions workflow builds Linux, macOS, and Windows binaries with Nix and creates the GitHub Release.

## Safety Rules

- Never release from a dirty working tree.
- Never tag a commit that is not on `main`; `.github/workflows/release.yml` rejects tags outside `origin/main` history.
- Never push until the version bump commit, tag, and Nix verification have been reviewed.
- Never use `git push --force` for a release.
- Keep `Cargo.toml`, `flake.nix`, and the git tag version in sync.
- Use non-interactive commands only; do not open editors or pagers.

## Version Sources

The release version is duplicated and must be updated in both places:

| File | Field |
| --- | --- |
| `Cargo.toml` | `[package] version = "<version>"` |
| `flake.nix` | `version = "<version>";` |

The git tag must be `v<version>`, for example `v0.1.0`.

## Workflow

### 1. Inspect Current State

Run read-only checks first:

```bash
git status --short
git branch --show-current
git fetch --tags origin
git tag --sort=-version:refname --list 'v*'
git log --oneline --decorate -10
```

Requirements:

- Working tree is clean, except for intentional release edits after this step.
- Current branch is `main`.
- Latest remote state is known before choosing the next version.

### 2. Determine the Next Version

Look up the previous tag:

```bash
previous_tag=$(git tag --sort=-version:refname --list 'v*' | head -n 1)
printf 'Previous tag: %s\n' "$previous_tag"
```

Choose the next version from the previous tag and requested bump:

- `patch`: increment `Z` in `vX.Y.Z`
- `minor`: increment `Y`, reset patch to `0`
- `major`: increment `X`, reset minor and patch to `0`
- explicit version: use the provided version after stripping an optional leading `v`

For prerelease tags such as `v0.0.2-alpha`, decide explicitly whether the next release remains prerelease or becomes stable. Do not silently drop or invent prerelease suffixes.

### 3. Update Versions

Update both files to the chosen version without the leading `v`:

```toml
# Cargo.toml
version = "<version>"
```

```nix
# flake.nix
version = "<version>";
```

Regenerate the lock file so the package metadata matches `Cargo.toml`:

```bash
cargo generate-lockfile
```

Verify all version sources match:

```bash
cargo_version=$(grep -m1 '^version = ' Cargo.toml | cut -d '"' -f2)
flake_version=$(grep -m1 'version = ' flake.nix | cut -d '"' -f2)
test "$cargo_version" = "$flake_version"
test "v$cargo_version" = "v<version>"
```

### 4. Verify Locally

Run the same Nix build path used by the native release jobs without updating the `result` symlink:

```bash
nix build --no-link --print-build-logs
```

If the Windows cross-build should also be verified before release, run:

```bash
nix build --no-link --print-build-logs .#graylog-cli-windows
```

Do not continue if any build fails.

### 5. Commit the Version Bump

Review the diff:

```bash
git diff -- Cargo.toml Cargo.lock flake.nix
```

Commit only the release version changes:

```bash
git add Cargo.toml Cargo.lock flake.nix
git commit -m "chore: bump version to <version>"
```

### 6. Tag the Release

Create an annotated tag on the version bump commit:

```bash
git tag -a "v<version>" -m "Release v<version>"
```

Verify the tag points at `HEAD` and that `HEAD` is on `main` history:

```bash
test "$(git rev-parse "v<version>^{commit}")" = "$(git rev-parse HEAD)"
git fetch origin main
git branch -r --contains HEAD | grep -q 'origin/main'
```

If `origin/main` does not contain `HEAD`, push the branch before pushing the tag.

### 7. Push

Push the release commit and tag:

```bash
git push origin main
git push origin "v<version>"
```

Alternatively, after confirming only the intended tag exists locally:

```bash
git push origin main --follow-tags
```

Pushing the `v*` tag triggers `.github/workflows/release.yml`.

### 8. Verify GitHub Release Workflow

Check the release workflow after pushing:

```bash
gh run list --workflow Release --limit 5
gh run watch
```

Expected workflow behavior:

- `verify-main` confirms the tag is in `origin/main` history.
- `build-linux` runs `nix build` and uploads `graylog-cli-linux-x86_64`.
- `build-mac` runs `nix build` and uploads `graylog-cli-macos-aarch64`.
- `build-windows` runs `nix build .#graylog-cli-windows` and uploads `graylog-cli-windows-x86_64.exe`.
- `release` creates the GitHub Release with all artifacts.

## Troubleshooting

**Versions do not match**

Update `Cargo.toml` and `flake.nix` to the same version, then run `cargo generate-lockfile` again.

**Nix build fails after Cargo version bump**

Check that `Cargo.lock` was regenerated and committed. Re-run `nix build --no-link --print-build-logs` before tagging.

**Release workflow fails in `verify-main`**

The tag commit is not reachable from `origin/main`. Delete the local tag if needed, move the release commit onto `main`, push `main`, recreate the tag, then push the tag.

**Tag already exists**

Do not overwrite published tags. Pick the next version unless the tag has not been pushed and can be safely deleted locally.
