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

### Loom-managed repo: land fixes where a sweep cannot take them

The check above catches an edit reverted *during* the run. It cannot catch the
likelier failure in a Loom-managed repo, which happens *after* it: a sweep runs
`check-main-clean.sh --quarantine`, which polices the **primary checkout's
working tree as a whole** — not the branch it happens to be on — and stashes
every uncommitted delta it finds so its worktrees get a clean base. Nothing is
destroyed (the stash is labelled `loom-quarantine: run=<sweep-id> issue=<N>`),
but the fixes this command reported as applied are off disk minutes later, and
nobody re-reads a summary that has already printed. Branching does not help —
the quarantine is branch-blind.

The related symptom, if you meet it: Loom's guard hooks deny writes into the
primary checkout while a managed worktree exists — `BLOCKED: ... resolves to the
main repository checkout ... but a Loom-managed worktree exists elsewhere`. When
that guard is disabled, or catches only the Bash-tool arm, the `Edit`/`Write`
fixes go in unblocked and are quarantined afterwards instead.

**Decide the destination before applying the first fix.** Detecting afterwards
means re-applying everything somewhere else:

```bash
root=$(git rev-parse --show-toplevel)
loom_managed=no
if [ -d "$root/.loom" ] && { [ -d "$root/.loom/worktrees" ] || pgrep -f loom-daemon >/dev/null 2>&1; }; then
  loom_managed=yes
fi
# Already inside a managed worktree? Then this is not the primary checkout,
# quarantine does not reach here, and nothing below applies.
[ -f "$root/.loom-managed" ] && loom_managed=no
```

`loom_managed=no` — every repo with no `.loom/` root, and any run from inside a
managed worktree — is the unchanged path: apply fixes in place, report them
exactly as before, and print none of the text below.

**If `loom_managed=yes`, choose a destination in this order:**

1. **Commit them in a dedicated worktree**, when the run is attached to an issue
   number: `./.loom/scripts/worktree.sh <issue-number>`, apply the fixes in the
   worktree it prints, commit them there, and report the branch and path. Always
   that helper, **never a bare `git worktree add`** — the helper is what writes
   the `.loom-managed` sentinel that authorizes later cleanup. It takes a numeric
   issue number and nothing else, so this arm exists only when the pass has one;
   do not invent a number to unlock it.
2. **Commit them on the current branch**, when the working tree was otherwise
   clean at the start of the run — then the commit holds your fixes and none of
   the operator's. Report the branch and the short sha. Add one clause when the
   current branch is the default branch: the primary clone is now a commit ahead
   of its upstream until someone pushes it.
3. **Leave them uncommitted and warn.** A dirty tree with no issue number lands
   here. Do not stash the operator's unrelated edits to manufacture a clean
   commit — that is the same working-tree rewrite this section exists to avoid.
   Print the warning once, on its own line:

   ```
   Loom-managed repo: uncommitted gitignore fixes in the primary checkout can be quarantined by a sweep — commit or stash them now.
   ```

**Name the destination in the report, not just the count.** `2 fixed` does not
say whether the fixes will still exist tomorrow:

```
Gitignore: 2 fixed on feature/issue-448 (worktree .loom/worktrees/issue-448, a1b2c3d)
Gitignore: 2 fixed, committed on main (a1b2c3d)
Gitignore: 2 fixed — uncommitted, at risk
```

If fixes did vanish this way they are recoverable rather than lost — send the
operator to the stash instead of re-applying blind:

```bash
git stash list | grep loom-quarantine
git stash show -p 'stash@{0}'     # replay with `git apply`
```

This too applies equally when the rule fixes are offered from [[all]]'s Audit
stage: same edits, same destination decision, and the Audit line names where
they landed.
