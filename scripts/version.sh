#!/usr/bin/env bash
# version.sh - Manage version across all VibeSQL workspace manifests
#
# Usage:
#   ./scripts/version.sh                    # Show current version
#   ./scripts/version.sh check              # Verify all manifests are in sync
#   ./scripts/version.sh set 0.1.5          # Set explicit version
#   ./scripts/version.sh set 0.1.5 --tag    # Set version, commit, and tag
#
# Covers [workspace.package] version in the root Cargo.toml and every
# internal `vibesql-* = { version = "X.Y.Z", ... }` dependency pin in the
# root and per-crate manifests (crates.io requires explicit versions on
# path dependencies at publish time).
#
# NOTE: pushing a v* tag triggers .github/workflows/release-crates.yml and
# release-pypi.yml (publishes to crates.io / PyPI). `--tag` only creates the
# tag locally; push it deliberately with: git push origin vX.Y.Z
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

manifest_files() {
  echo "$REPO_ROOT/Cargo.toml"
  ls "$REPO_ROOT"/crates/*/Cargo.toml
}

get_version() {
  # [workspace.package] version in the root manifest is the source of truth.
  # Anchored on ^...$ so nested-table or dependency `version = "..."` lines
  # elsewhere in the file never match.
  grep -E '^version = "[0-9]+\.[0-9]+\.[0-9]+"$' "$REPO_ROOT/Cargo.toml" \
    | grep -oE '[0-9]+\.[0-9]+\.[0-9]+'
}

check_versions() {
  local expected
  expected=$(get_version)
  local all_match=true
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
  done < <(manifest_files)
  if [[ "$all_match" == "true" ]]; then
    echo "All manifests in sync at $expected"
  else
    echo "Version drift detected" >&2
    exit 1
  fi
}

set_version() {
  local new="$1"
  if [[ ! "$new" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "error: version must be X.Y.Z, got '$new'" >&2
    exit 2
  fi
  # Root [workspace.package] version (anchored, see get_version).
  sed -i.bak -E 's/^version = "[0-9]+\.[0-9]+\.[0-9]+"$/version = "'"$new"'"/' "$REPO_ROOT/Cargo.toml"
  rm "$REPO_ROOT/Cargo.toml.bak"
  # Internal dependency pins in every manifest.
  while IFS= read -r file; do
    sed -i.bak -E 's/(vibesql-[a-z0-9-]+ = \{ version = ")[0-9]+\.[0-9]+\.[0-9]+(")/\1'"$new"'\2/' "$file"
    rm "$file.bak"
  done < <(manifest_files)
  echo "Set version to $new"
}

usage() {
  sed -n '2,8p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

case "${1:-show}" in
  show|"")
    get_version
    ;;
  check)
    check_versions
    ;;
  set)
    new="${2:?usage: set X.Y.Z [--tag]}"
    set_version "$new"
    if [[ "${3:-}" == "--tag" ]]; then
      cd "$REPO_ROOT"
      git add Cargo.toml crates/*/Cargo.toml
      git commit -m "chore(release): prepare v$new release"
      git tag "v$new"
      echo "Committed and tagged v$new (local only)"
      echo "Push deliberately to trigger crates.io/PyPI release workflows:"
      echo "  git push origin HEAD && git push origin v$new"
    fi
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "unknown command: $1" >&2
    usage
    ;;
esac
