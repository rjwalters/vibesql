---
name: "gitignore"
description: "Audit gitignore rules — find over-ignored files and under-ignored build artifacts"
domain: repo
type: command
user-invocable: true
---

# /repo:gitignore — Gitignore Audit

Check that gitignore rules are appropriate for this repository. Catches files
that shouldn't be ignored and build artifacts that should be.

## Usage

```
/repo:gitignore                  # Full repo — apply clear rule fixes, report as you go
/repo:gitignore data/            # Check one subtree
/repo:gitignore --ask            # Review findings and confirm before editing
```

## Context First

Determine whether the repo is public or private before judging rules
(`gh repo view --json isPrivate --jq .isPrivate`, or ask the user if there is
no GitHub remote). The right answer differs:
- **Private repos** often want data files, docs, and notes *tracked* — flag
  rules that hide them.
- **Public repos** often want those same files *ignored* — flag tracked files
  that look like they leaked in (credentials, dumps, personal notes are
  critical findings either way).

## What It Checks

### 1. Over-Ignored Files
Flag gitignore rules that exclude things that look like real content:
- Data files (.yaml, .json, .csv) that aren't build output
- Documentation or notes
- Configuration that isn't secrets

**Always keep ignored, in any repo:**
- `.env` files and anything credential-like
- `node_modules/`, `.venv/`, `__pycache__/`
- Build output (`dist/`, `build/`, `target/`, `*.pyc`)
- IDE files (`.vscode/`, `.idea/`)
- OS files (`.DS_Store`)

### 2. Under-Ignored Files
Find tracked files that are probably build artifacts:
- `*.pyc`, `__pycache__/`
- `dist/`, `build/`, coverage output
- Large binaries that look generated (`.o`, `.so`, `.whl`)

### 3. Gitignore Hygiene
- Redundant rules (already covered by a parent `.gitignore`)
- Rules that match zero files (stale after cleanup)
- Scattered `.gitignore` files that could be consolidated

**Do not flag `X` and `X/` as duplicates without verification.** A trailing
slash restricts a gitignore pattern to directories — it never matches a
symlink, even one that points at a directory (`man gitignore`: "If there is a
separator at the end of the pattern then the pattern will only match
directories"). The two rules are therefore not interchangeable:

| Path at `X`           | Matched by `X` | Matched by `X/` |
|-----------------------|:--------------:|:---------------:|
| Real directory        | yes            | yes             |
| Symlink (to anything) | yes            | **no**          |
| Regular file          | yes            | **no**          |

`X` and `X/` in the same file are true duplicates only when `X` is guaranteed
never to be a symlink. Verify it — don't eyeball it:

```bash
[ -d "X" ] && [ ! -L "X" ]   # true only for a real, non-symlink directory
```

`[ -d "X" ]` alone is **not** sufficient: it follows symlinks, so a symlink to
a directory passes it. If the check fails, `X` doesn't exist to test, or `X`
could plausibly become a symlink in this repo (vendored trees, external
volumes, build outputs relocated to another disk), the pair is **not
redundant** — keep both rules in the suggested-fix output and report the pair
as intentional rather than collapsing it. Dropping the bare rule un-ignores any
symlink at that path, because `X/` alone won't re-cover it. This caused a live
regression: `.lake` + `.lake/` were deduped to `.lake/`, unignoring a `.lake`
symlink (rjwalters/lean-genius#43683).

### 4. Large Untracked Files
Find untracked files >1 MB that might need a decision:
- Should they be tracked? (data files, docs)
- Should they be gitignored? (build output, caches)
- Should they live outside the repo? (measurement data, large datasets —
  object storage, LFS, or a NAS)

## Interaction

For each `.gitignore` file, show:
- Current rules and what they match
- Suggested additions or removals
- Files affected by changes

By default, apply the clear-cut rule changes (adding an obvious build-artifact
ignore, removing a rule that hides real content) and report each edit — gitignore
changes are fully git-reversible. Leave anything ambiguous, or that would change
whether a **tracked** file stays tracked, as a reported recommendation. Under
`--ask`, confirm every edit before writing.

### Verify after write

Applying a `.gitignore` edit is not proof it survived. A concurrent writer —
another agent working in the same clone, a background `git stash` or
`git checkout --`, a pre-commit hook, a Loom sweep quarantining the primary
clone's working tree — can revert a file between the moment you fix it and the
moment you report it, leaving this command claiming a fix that is no longer on
disk.

So immediately after applying each fix, and **before counting it as applied**,
re-read the changed region of the file and confirm your specific edit is
present. `git diff -- <path>` / `git status --porcelain -- <path>` is a cheap
first pass, but only proves the path differs from HEAD — it cannot distinguish
your edit from someone else's, so it must not be the sole check when the file
may carry other uncommitted changes.

This check is **unconditional** — run it whether or not you have any reason to
suspect a concurrent writer. Detecting a daemon first would be racy (one can
start right after the check), and in a repo with no concurrent writer the check
always finds the edit still applied, so nothing about the reported output
changes.

If a fix is gone on re-check, report it on its own line as **reverted after
apply — needs re-run**. Do not silently re-apply it, and do not count it in the
fixed total — that total must only ever include edits confirmed still on disk.

This applies equally when these rule fixes are offered from [[all]]'s Audit
stage rather than from `/repo:gitignore` directly — same edits, same check.
