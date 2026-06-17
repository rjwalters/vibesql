#!/usr/bin/env bash
# version.sh - Manage version across all VibeSQL version-bearing files
#
# Implements the contract documented in `.claude/commands/loom/release.md`
# ("scripts/version.sh interface" section) so /loom:release's Phase 2a detection
# and Phase 5 dispatch drive the bump fully without operator intervention.
#
# Usage:
#   ./scripts/version.sh                              # Show current version
#   ./scripts/version.sh list                         # List version-bearing files (one per line)
#   ./scripts/version.sh check                        # Verify all version-bearing files agree
#   ./scripts/version.sh bump <level> [--tag]         # Bump (patch/minor/major), commit, optionally tag
#   ./scripts/version.sh set <X.Y.Z> [--tag]          # Set explicit version, commit, optionally tag
#
# Version-bearing files (all updated atomically by `set` / `bump`):
#   1. Cargo.toml                                          ([workspace.package] version)
#   2. crates/*/Cargo.toml                                 (internal `vibesql-* = { version = ... }` pins)
#   3. pyproject.toml                                      (root, drives the `vibesql` PyPI wheel)
#   4. crates/vibesql-python-bindings/pyproject.toml       (Python bindings manifest)
#   5. Cargo.lock                                          (refreshed via `cargo update -w`)
#
# NOTE: Cargo.lock is gitignored in this repo — it is regenerated locally and is
# NOT staged or committed by `commit_and_tag`. Files 1-4 are the tracked version
# sources committed atomically by the `--tag`/commit path.
#
# NOTE: pushing a v* tag triggers .github/workflows/release-crates.yml and
# release-pypi.yml (publishes to crates.io / PyPI). `--tag` only creates the
# tag locally; push it deliberately with: git push origin vX.Y.Z
#
# DO NOT bump:
#   - crates/vibesql-sqllogictest/Cargo.toml (pinned to upstream sqllogictest 0.28.x)
#   - web-demo/package.json (tracks its own version independently)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# Cargo workspace + member manifests (where workspace.package version + internal pins live).
cargo_manifest_files() {
  echo "$REPO_ROOT/Cargo.toml"
  ls "$REPO_ROOT"/crates/*/Cargo.toml
}

# All version-bearing files, one per line. Consumed by `/loom:release` Phase 5 step 2.
list_files() {
  echo "$REPO_ROOT/Cargo.toml"
  ls "$REPO_ROOT"/crates/*/Cargo.toml
  echo "$REPO_ROOT/pyproject.toml"
  echo "$REPO_ROOT/crates/vibesql-python-bindings/pyproject.toml"
  echo "$REPO_ROOT/Cargo.lock"
}

get_version() {
  # [workspace.package] version in the root manifest is the source of truth.
  # Anchored on ^...$ so nested-table or dependency `version = "..."` lines
  # elsewhere in the file never match.
  grep -E '^version = "[0-9]+\.[0-9]+\.[0-9]+"$' "$REPO_ROOT/Cargo.toml" \
    | grep -oE '[0-9]+\.[0-9]+\.[0-9]+'
}

# Pull the `version = "X.Y.Z"` line from a pyproject.toml-style file.
get_pyproject_version() {
  local file="$1"
  grep -m1 -E '^version = "[0-9]+\.[0-9]+\.[0-9]+"' "$file" \
    | grep -oE '[0-9]+\.[0-9]+\.[0-9]+'
}

# Pull the version field for a named package from Cargo.lock.
get_cargo_lock_version() {
  local pkg="$1"
  awk -v pkg="$pkg" '
    /^\[\[package\]\]/ { in_pkg=0; name=""; next }
    /^name = / { gsub(/"/, "", $3); name=$3; if (name == pkg) in_pkg=1; next }
    in_pkg && /^version = / { gsub(/"/, "", $3); print $3; exit }
  ' "$REPO_ROOT/Cargo.lock"
}

check_versions() {
  local expected
  expected=$(get_version)
  local all_match=true

  # Cargo manifests: workspace.package version + internal `vibesql-*` dep pins.
  while IFS= read -r file; do
    local rel="${file#"$REPO_ROOT"/}"
    local stray
    stray=$(grep -E 'vibesql-[a-z0-9-]+ = \{ version = "[0-9]+\.[0-9]+\.[0-9]+"' "$file" \
      | grep -oE 'version = "[0-9]+\.[0-9]+\.[0-9]+"' \
      | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' \
      | grep -v "^$expected\$" || true)
    if [[ -n "$stray" ]]; then
      printf "  %-50s DRIFT: %s (expected %s)\n" "$rel" "$(echo "$stray" | sort -u | tr '\n' ' ')" "$expected" >&2
      all_match=false
    else
      local count
      count=$(grep -cE 'vibesql-[a-z0-9-]+ = \{ version = "' "$file" || true)
      printf "  %-50s %s (%s internal pins)\n" "$rel" "$expected" "$count"
    fi
  done < <(cargo_manifest_files)

  # pyproject.toml (root) — drives the `vibesql` PyPI wheel.
  local py_root
  py_root=$(get_pyproject_version "$REPO_ROOT/pyproject.toml" || true)
  if [[ "$py_root" != "$expected" ]]; then
    printf "  %-50s DRIFT: %s (expected %s)\n" "pyproject.toml" "$py_root" "$expected" >&2
    all_match=false
  else
    printf "  %-50s %s (root pyproject)\n" "pyproject.toml" "$py_root"
  fi

  # crates/vibesql-python-bindings/pyproject.toml.
  local py_bind
  py_bind=$(get_pyproject_version "$REPO_ROOT/crates/vibesql-python-bindings/pyproject.toml" || true)
  if [[ "$py_bind" != "$expected" ]]; then
    printf "  %-50s DRIFT: %s (expected %s)\n" \
      "crates/vibesql-python-bindings/pyproject.toml" "$py_bind" "$expected" >&2
    all_match=false
  else
    printf "  %-50s %s (python bindings)\n" \
      "crates/vibesql-python-bindings/pyproject.toml" "$py_bind"
  fi

  # Cargo.lock — every `vibesql-*` workspace crate has an entry. Spot-check the
  # top-level `vibesql` binary; if any internal crate drifted, `set_version`'s
  # `cargo update -w` step is the recovery.
  local lock_v
  lock_v=$(get_cargo_lock_version "vibesql" || true)
  if [[ "$lock_v" != "$expected" ]]; then
    printf "  %-50s DRIFT: %s (expected %s)\n" "Cargo.lock (vibesql)" "$lock_v" "$expected" >&2
    all_match=false
  else
    printf "  %-50s %s (Cargo.lock vibesql package)\n" "Cargo.lock" "$lock_v"
  fi

  if [[ "$all_match" == "true" ]]; then
    echo "All version-bearing files in sync at $expected"
  else
    echo "Version drift detected" >&2
    exit 1
  fi
}

# Update version across all version-bearing files. Internal helper; called by
# both `set` and `bump`. Does NOT commit or tag — the caller decides.
write_version() {
  local new="$1"
  if [[ ! "$new" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: version must be X.Y.Z, got '$new'" >&2
    exit 2
  fi

  # 1. Workspace.package version (anchored, see get_version).
  sed -i.bak -E 's/^version = "[0-9]+\.[0-9]+\.[0-9]+"$/version = "'"$new"'"/' "$REPO_ROOT/Cargo.toml"
  rm "$REPO_ROOT/Cargo.toml.bak"

  # 2. Internal `vibesql-*` dependency pins in every Cargo manifest.
  while IFS= read -r file; do
    sed -i.bak -E 's/(vibesql-[a-z0-9-]+ = \{ version = ")[0-9]+\.[0-9]+\.[0-9]+(")/\1'"$new"'\2/' "$file"
    rm "$file.bak"
  done < <(cargo_manifest_files)

  # 3. Root pyproject.toml.
  sed -i.bak -E 's/^version = "[0-9]+\.[0-9]+\.[0-9]+"/version = "'"$new"'"/' "$REPO_ROOT/pyproject.toml"
  rm "$REPO_ROOT/pyproject.toml.bak"

  # 4. Python-bindings pyproject.toml.
  sed -i.bak -E 's/^version = "[0-9]+\.[0-9]+\.[0-9]+"/version = "'"$new"'"/' \
    "$REPO_ROOT/crates/vibesql-python-bindings/pyproject.toml"
  rm "$REPO_ROOT/crates/vibesql-python-bindings/pyproject.toml.bak"

  # 5. Refresh Cargo.lock so every `vibesql-*` workspace crate picks up the new version.
  # Run from REPO_ROOT so cargo finds the workspace manifest.
  (cd "$REPO_ROOT" && cargo update -w >/dev/null 2>&1) \
    || { echo "error: 'cargo update -w' failed" >&2; exit 3; }

  echo "Set version to $new across $(list_files | wc -l | tr -d ' ') file(s)"
}

# Commit + (optionally) tag the version bump. Mirrors the prior --tag semantics.
commit_and_tag() {
  local new="$1"
  local want_tag="$2"
  cd "$REPO_ROOT"
  # Cargo.lock is intentionally omitted: it is gitignored in this repo (regenerated
  # locally via `cargo update -w`, never committed). Adding it here would make
  # `git add` exit non-zero on the ignored path and abort the script under
  # `set -euo pipefail` before the commit/tag runs.
  git add Cargo.toml crates/*/Cargo.toml pyproject.toml \
    crates/vibesql-python-bindings/pyproject.toml
  git commit -m "chore(release): prepare v$new release"
  if [[ "$want_tag" == "--tag" ]]; then
    git tag "v$new"
    echo "Committed and tagged v$new (local only)"
    echo "Push deliberately to trigger crates.io/PyPI release workflows:"
    echo "  git push origin HEAD && git push origin v$new"
  else
    echo "Committed v$new (local only, no tag created)"
  fi
}

set_version() {
  local new="$1"
  local tag_flag="${2:-}"
  write_version "$new"
  if [[ -n "$tag_flag" ]]; then
    commit_and_tag "$new" "$tag_flag"
  fi
}

# Compute the next semver from the current version + a bump level.
next_version() {
  local current="$1"
  local level="$2"
  local major minor patch
  IFS='.' read -r major minor patch <<< "$current"
  case "$level" in
    major) major=$((major+1)); minor=0; patch=0 ;;
    minor) minor=$((minor+1)); patch=0 ;;
    patch) patch=$((patch+1)) ;;
    *) echo "error: bump level must be patch|minor|major, got '$level'" >&2; exit 2 ;;
  esac
  echo "$major.$minor.$patch"
}

bump_version() {
  local level="$1"
  local tag_flag="${2:-}"
  local current new
  current=$(get_version)
  new=$(next_version "$current" "$level")
  echo "Bumping $current -> $new ($level)"
  write_version "$new"
  # `bump` always commits; `--tag` decides whether to also tag.
  commit_and_tag "$new" "$tag_flag"
}

usage() {
  sed -n '2,18p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

case "${1:-show}" in
  show|"")
    get_version
    ;;
  list)
    list_files
    ;;
  check)
    check_versions
    ;;
  set)
    new="${2:?usage: set X.Y.Z [--tag]}"
    set_version "$new" "${3:-}"
    ;;
  bump)
    level="${2:?usage: bump <patch|minor|major> [--tag]}"
    bump_version "$level" "${3:-}"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "unknown command: $1" >&2
    usage
    ;;
esac
