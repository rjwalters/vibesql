---
name: "update-tools"
description: "Check installed tool packages (Loom, Anvil, Repo Skills, …) against their source repos and offer to update"
domain: repo
type: command
user-invocable: true
---

# /repo:update-tools — Tool Package Updates

Find every tool package installed into this repo by an Anvil/Loom-style
installer, compare each against the latest version of its source, and offer
to update the stale ones.

Scope is *installer-managed tool packages* only. Third-party dependency
currency — npm/cargo/pip packages and GitHub Actions, i.e. Dependabot setup and
Dependabot PR triage — is [[deps]], not this command: there is no local source
clone to diff against, so it needs a different comparison model.

## Usage

```
/repo:update-tools               # Report, then offer updates (commit + land on the default branch)
/repo:update-tools --check       # Report only, never writes
/repo:update-tools loom          # Only check/update one tool
/repo:update-tools --no-commit   # Update the working tree but leave it uncommitted for review
```

An update runs the tool's own installer or updater (executing code from its
source repo and rewriting `.claude/`), so unlike the safe-fix hygiene commands
this one is **not** auto-applied — it reports and confirms before updating.
`--check` is the report-only form.

Once an update is confirmed, it is committed and landed on the default branch
(`main`) by default — it does **not** push, and it never folds a pre-existing
dirty working tree into the update commit. Pass `--no-commit` (alias
`--stage-only`) to restore the old behavior of leaving the changes uncommitted
for manual review. See step 5 and the Safety Rules for details.

## Steps

### 1. Discover installed tools

Tools in this family record their install in a metadata file. Sweep for it
rather than listing known paths — a fixed list structurally cannot find a
family member added after this doc was last updated, which is exactly how
`.kct/install-metadata.json` went undiscovered before (repo#165):

```bash
find . -maxdepth 4 -name "install-metadata.json" \
  -not -path "*/node_modules/*" -not -path "*/.venv/*" 2>/dev/null
```

`-maxdepth 4` reaches every known tool root (`.loom/`, `.anvil/`, `.kct/` at
depth 2, and the two-levels-deeper `.claude/skills/*/` at depth 4) and any
future tool that follows the same shallow layout, without needing a doc edit
when one is added.

Key names vary by tool (`version` vs `loom_version` / `anvil_version` /
`kct_version`, `source` vs `loom_source` / `anvil_source`) — read whichever
variant is present. Each file gives: installed version, installed commit,
install date, and (for the "prefer local source clone" fast path) the path of
the local source clone it was installed from — **except kicad-tools**, whose
shape differs; see below.

**The metadata layout and the source-resolution order are normative in the
[tool-package installer contract][contract]** — requirements **C5** (tracked
metadata: `version`, `commit`, `layout_version`, never a path or timestamp) and
**C6** (the gitignored `.install-local.json` sidecar, and the sidecar → legacy
inline → unknown resolution order, including the repo#96 "sidecar deleted by a
pull" signature and the exact suggestion to append). Read C6 before implementing
this step; do not re-derive the rules from what a given tool happens to do.

Two things are *this command's* behavior rather than the contract's, so they
stay here: an unresolved source means **skip to the GitHub check in step 2**
(not an error), and the repo#96 signature gets its own distinct report row —
see step 3.

[contract]: https://github.com/rjwalters/repo/blob/main/INSTALLER-CONTRACT.md

**kicad-tools does not conform to C5/C6 — resolve its shape directly instead
of via the sidecar ladder.** `.kct/install-metadata.json` carries `kct_version`
/ `kct_commit` (not `version` / `commit`), and it never writes a sidecar: it
always records `source_mode` (`"path"` or `"git"`) and `source_ref` inline in
the tracked file.

- `source_mode: "path"` — `source_ref` **is** the local source clone path; use
  it directly as `<source>` in step 2, no sidecar lookup needed.
- `source_mode: "git"` (kicad-tools' default) — there is no local clone at all;
  `source_ref` is a `<git-url>@<tag-or-rev>` string, not a path. This is the
  normal/default install shape, not a degraded case — treat it exactly like
  "source unknown" above and skip straight to the GitHub check in step 2. Never
  report it as a missing/broken source repo.

Known family members: Loom (`.loom/`), Anvil (`.anvil/`), Repo Skills
(`.claude/skills/repo/`), kicad-tools, and anything else that follows the same
metadata pattern. Report any metadata file found even if the tool is
unrecognized.

**Detect dev installs before comparing versions.** `install.sh --dev .`
**symlinks** a tool's files directly into `.claude/` instead of copying them —
the installed surface *is* the source clone, so stamped-version comparison is
meaningless by construction: it cannot be stale, and it cannot become stale by
the source moving ahead, because the "installed" files and the source files
are the same inode. Detect this per tool, checking both signals (either one is
sufficient — don't require both):

- **Flag**: read `dev` (or the tool's equivalent field) from the metadata
  file, e.g. Repo Skills' `install-metadata.json` carries `"dev": true`.
- **Structural fallback**, for metadata predating the flag: test whether the
  tool's primary installed file is a symlink, `[[ -L <path> ]]` — e.g. one of
  the paths in the metadata's own `commands`/`installed_files` list. This is
  the same check `resync-installed.sh` already uses to recognize a dev
  destination and skip overwriting it (`scripts/repo/resync-installed.sh:317`,
  `... symlinked (dev-mode install) ...`).

A tool flagged dev by either check is **dev-mode** for the rest of this
command: it skips the STALE/current comparison in step 2, gets its own report
status in step 3, and is never offered an update in step 4.

### 2. Determine the latest version of each

**Dev-mode tools (step 1) skip this comparison entirely** — do not run the
stamped-version-vs-latest check below for them. Instead, check only whether
the **source clone itself** is behind its own remote — that's the one
meaningful staleness a dev install can have, since the install *is* the
source:

```bash
git -C <source> fetch origin --quiet
git -C <source> log --oneline HEAD..origin/HEAD | wc -l    # source clone itself behind?
```

A non-zero count is reported in step 3 as the *source clone* being behind
(fixed with a plain `git -C <source> pull` on that clone) — never as the
*install* being STALE, and never as grounds to offer step 4's update flow.

For every non-dev tool, prefer the local source clone recorded in the
metadata:

```bash
git -C <source> fetch origin --quiet
git -C <source> log --oneline HEAD..origin/HEAD | wc -l    # source clone itself behind?
# Version at origin: VERSION file, package.json, or pyproject.toml on origin/HEAD
git -C <source> show origin/HEAD:VERSION 2>/dev/null
# How far the INSTALLED commit is behind the source's origin/HEAD
git -C <source> log --oneline <installed-commit>..origin/HEAD | wc -l
```

**Record two numbers per non-dev tool, not one — version drift AND commit
drift.** Version equality alone systematically under-reports staleness:
upstream routinely merges a day of work without bumping VERSION, so a tool
whose stamped version equals the VERSION at `origin/HEAD` can still be dozens
of commits behind the code that will eventually ship under that same version
number. In a real run that reported two tools `current`, one was 24 and the
other 7 commits behind, and the misleading `current` stood for a whole working
day until upstream happened to cut a release (repo#291). So carry both into
step 3:

1. **Version drift** — the metadata's installed `version` vs the VERSION at
   `origin/HEAD` (the comparison already shown above).
2. **Commit drift** — `git -C <source> log --oneline <installed-commit>..origin/HEAD | wc -l`,
   where `<installed-commit>` is the metadata's `commit` field. That field is
   tracked, per-tool, install-time metadata required by the [contract][contract]
   (**C5**), so it is already available from step 1 — no new metadata is needed.
   Use the tool's key variant where it differs (`kct_commit` for kicad-tools,
   `loom_commit` / `anvil_commit` for legacy inline shapes).

**Commit drift is only computable when both inputs exist.** It needs a local
source clone *and* a recorded installed commit that the clone can actually
resolve. Record the distance as **unknown** — and report the tool on the
version comparison alone — whenever any of these holds:

- the source clone is missing, or was never resolved (the `source_mode: "git"`
  and "source unknown" paths in step 1), so the GitHub fallback below is the
  only comparison available;
- the metadata predates the `commit` field, or the field is empty;
- the clone cannot resolve the recorded commit (upstream force-pushed or
  rebased it away).

Verify resolvability before measuring, rather than trusting `wc -l` on a failed
`git log`:

```bash
git -C <source> cat-file -e <installed-commit>^{commit} 2>/dev/null \
  || echo "installed commit not in source clone — commit drift unknown"
```

Never report an uncomputable distance as `0 commits behind`, and never drop the
caveat silently: a bare `current` that was never actually checked against the
source HEAD is the exact failure this comparison exists to remove.

If the source clone no longer exists, fall back to GitHub:
`gh api repos/<owner>/<repo>/tags --jq '.[0].name'` or the latest release.
If neither works, mark the tool UNKNOWN rather than guessing.

### 3. Report

```
TOOL PACKAGES
=============
| Tool        | Installed        | Latest  | Status      |
|-------------|------------------|---------|-------------|
| loom        | 0.9.1 (Jun 4)    | 0.10.6  | STALE       |
| anvil       | 0.9.0 (Jul 1)    | 0.9.0   | current     |
| other-tool  | 1.2.0 (commit abc1234) | 1.2.0 | current, but 24 commits behind source HEAD |
| repo-skills | 0.8.0 (Aug 9)    | —       | dev (symlinked to /Users/you/GitHub/repo) |
| kicad-tools | 2.3.0 (May 20)   | ?       | source repo missing — clone it? |
| some-tool   | 1.2.0 (Jun 30)   | ?       | sidecar missing — re-run installer? |
```

**A non-dev tool gets one of three statuses, not two.** Version equality by
itself is not "current" — resolve the status from *both* numbers step 2
recorded:

| Version vs `origin/HEAD` | Installed commit vs `origin/HEAD` | Status |
|--------------------------|-----------------------------------|--------|
| behind | not consulted — version drift already decides it | `STALE` |
| equal | 0 commits behind | `current` |
| equal | N > 0 commits behind | `current, but N commits behind source HEAD` |
| equal | not computable (step 2) | `current (commit drift unknown — <why>)` |

The third row is what this distinction exists for. It is **not** a claim that
the install is broken — the released version really does match — it says the
source has merged work that has not been cut into a release yet, which the
operator can take right now (step 4) because the installers are idempotent.
Reporting it as a plain `current` is what hid 24 unreleased commits for a full
working day (repo#291), and it is why "is this tool even included in the check?"
was a reasonable question to ask of a report that *had* included it.

When the commit distance is non-zero, put the installed short SHA in the
`Installed` cell (`1.2.0 (commit abc1234)`) instead of the install date, so the
operator can run the `git -C <source> log` range themselves without re-reading
the metadata file. `STALE` rows do not need a commit count appended: the version
bump is already the actionable signal there. The `<why>` in the unknown row is
the specific reason from step 2 (`no source clone`, `no recorded commit`,
`commit not in source clone`) — never an unqualified `current`.

The last two rows are **different** failure modes, so report them distinctly:
`source repo missing` means the recorded source clone path no longer exists on
disk, while `sidecar missing` is the signature check above (installed here once,
but the machine-local pointer is gone — typically deleted by pulling an
untracking commit, repo#96).

**A dev-mode tool (step 1) always gets its own `dev (symlinked to <source>)`
status row — never `current`, never `STALE`, and never the commit-drift status
above.** The three-status table is the *non-dev* path only: a dev install has no
stamped copy to be behind its source, so its source clone's own distance from
its remote is reported inside the `dev (…)` row (below), not as
`current, but N commits behind source HEAD`. `current` would only be a
coincidence for a symlinked install (the files are the source, not a copy that
happens to match it), and reporting it that way hides from the operator that
this install is not an ordinary copy. Leave `Latest` as `—`: there is no
"latest for this install" to compare against, only the source clone's own
position relative to its remote. If step 2 found the source clone itself
behind its remote, fold that into the same row instead of a separate STALE
row, e.g.:

```
| repo-skills | 0.8.0 (Aug 9)    | —       | dev (symlinked; source clone 3 commits behind origin) |
```

That is actionable (`git -C <source> pull`) without implying the *install*
needs — or can receive — an update.

Where a changelog exists in the source repo, summarize what changed between
the installed and latest versions for non-dev tools.

### 4. Update (with confirmation)

**Never offer a dev-mode tool (step 1/3) an update here — not even when its
source clone reports behind in step 3.** There is nothing to update: the
installed files already are the source clone. Running an installer/updater
over a dev install anyway would replace its symlinks with rendered copies,
silently ending the live-editing setup `--dev` exists to provide, with no
signal to the operator that it happened — exactly the harm this whole check
exists to prevent. If the report showed the source clone behind its remote,
the fix is a plain `git -C <source> pull` on that clone, run by the operator
directly — outside this update flow, not through it.

**Commit drift is offered on exactly the same terms as version drift.** A tool
reported `current, but N commits behind source HEAD` (step 3) goes into the same
confirmation prompt as a `STALE` one, with its distance shown, and is updated
only if the user approves it. The update mechanism does not care which signal
triggered it: the resync/installer paths below are idempotent and re-runnable
(Safety Rule 2) and they install whatever is at the source clone's
`origin/HEAD` — which *is* the unreleased work the commit-drift row is
reporting. Two things do not change: it is never auto-applied just because an
unchanged version number makes it look like a no-op (Safety Rule 1 applies
unchanged — it is a real code change), and a tool whose commit drift came back
**unknown** is never offered an update on that basis, because nothing was
actually compared.

For each non-dev tool the user approves — whether it was reported `STALE`
(version drift) or `current, but N commits behind source HEAD` (commit drift) —
update the source clone first, then run that tool's own update mechanism — its
dedicated updater where it ships one, otherwise its installer. Never hand-copy
files:

```bash
git -C <source> pull --ff-only
# Loom:        <this-repo>/.loom/scripts/resync-installed.sh --dry-run   # preview drift
#              <this-repo>/.loom/scripts/resync-installed.sh             # apply once confirmed
# Repo Skills: <this-repo>/.claude/skills/repo/scripts/resync-installed.sh --dry-run
#              <this-repo>/.claude/skills/repo/scripts/resync-installed.sh
# Anvil:       <source>/scripts/install-anvil.sh <this-repo>
# kicad-tools: <source>/scripts/install-kct.sh <this-repo>
# Unknown tools: look for <tool-root>/scripts/resync-installed.sh first (contract
#                C7); failing that, install.sh / scripts/install-*.sh in the source
```

**Prefer a tool's C7 resync over its installer.** [Contract][contract] C7 gives
every conforming tool the same consumer-side entry point —
`<tool-root>/scripts/resync-installed.sh`, with `--dry-run` / `--quiet` and the
same exit codes (`0` in sync, `2` drift found, `1` error) — so the same two
commands drive any tool that ships one. Run `--dry-run` first, report it, then
apply. Loom and Repo Skills ship one today; Anvil and kicad-tools do not, so
their rows re-run the installer (see the verified note below).

**The resync rows resolve their target from cwd, not from an argument.** Note
the path: a resync script lives in the **target** repo's tool root, not in the
source clone, unlike every other row above. That `<this-repo>/` prefix documents
**which copy of the script to run**, not a target argument the script consumes —
the asymmetry with the sibling rows is deliberate. In every other row the
trailing `<this-repo>` is a positional argument that **selects** the repo the
installer acts on. So do **not** "fix" a resync row to look like its siblings by
appending a bare target path: Loom's script rejects a positional with exit `1`
(its arg loop matches only `--dry-run`/`-n`, `--quiet`/`-q`, `--allow-worktree`,
`--help`/`-h`), and so does Repo Skills' (which takes an explicit `--target
<path>` instead) — in both cases with an error that does not obviously point back
to the cause.

What guarantees cwd is the target repo at this point is that `/repo:update-tools`
runs in the target repo's working directory and nothing earlier in step 4 changes
it — the source clone is only ever reached through `git -C <source> …`, never a
`cd`. Any future refactor that moves these lines must preserve that invariant, or
they will silently resync whichever repo cwd happens to be.

- **Loom** resolves its target via `git rev-parse --git-common-dir`, which points
  at the **primary** checkout even from a linked worktree — so running it from a
  worktree writes to the main checkout. It re-stamps `loom_version` /
  `loom_commit` / `last_resync` into `.loom/install-metadata.json` on a
  successful non-dry-run. `<source>/install.sh --quick -y <this-repo>` is **not**
  an update command for it: Loom's installer refuses a non-interactive reinstall
  over an existing `.loom/` and exits with an error, which is the only situation
  this step ever runs in.
- **Repo Skills** resolves its target via `git rev-parse --show-toplevel`, so
  writes land in the worktree you are standing in; there is no `--allow-worktree`
  because there is nothing to escape. It re-stamps `version` / `commit` into the
  tracked `install-metadata.json` and `last_resync` into the gitignored sidecar
  (the C5/C6 split), and resolves its source clone with the same sidecar → legacy
  inline order documented in step 1.

Reinstall is the **destructive fallback**, used only when resync cannot resolve
the drift:

```bash
# Destructive — uninstalls the existing Loom payload before writing the new version.
# Inventory and back up project-owned Loom hooks, scripts, and agent configuration first.
<source>/install.sh --quick -y --confirm-reinstall <this-repo>
```

Confirm that separately with the user; do not escalate to it just because a
resync pass exited non-zero — see the re-run caveat first.

**Anvil and kicad-tools rows verified correct as written (issue #135) — do not
re-investigate.** Unlike Loom, neither installer refuses a non-interactive
reinstall over an existing install, so `<source>/scripts/install-anvil.sh
<this-repo>` and `<source>/scripts/install-kct.sh <this-repo>` both succeed on
a second run and need no resync-equivalent or destructive-fallback split:

- **Anvil** (`rjwalters/anvil` `scripts/install-anvil.sh`, checked at `8302890`):
  Stage 3's "active-install guard" only sets `UPGRADE=true` when `.anvil/`
  already exists and proceeds — no exit, no confirmation gate bypassed by
  `-y`. The installer's own `--help` text tells consumers to "re-run
  `install-anvil.sh .` from the anvil checkout" to upgrade.
- **kicad-tools** (`rjwalters/kicad-tools` `scripts/install-kct.sh`, checked at
  `87561cf`): the header comment states outright "Re-running the installer is
  the upgrade/idempotency path: a second run with the same args adds no
  duplicate CLAUDE.md block and no duplicate dependency" — Stage 5 explicitly
  no-ops when the dependency is already present and up to date.

**Re-run caveat: `resync-installed.sh` syncs itself.** The script is part of the
`.loom/scripts/` payload it updates, so the copy that starts the run is the
*old* one. A stale copy carrying a bug can die partway through (observed going
0.16.0 → 0.18.0: `line 509: verb_past: unbound variable`) after it has already
written the newer script to disk. Re-running it once is expected to pick up the
freshly-synced copy and complete cleanly (in that case, 70 further files
updated). Treat a single failed pass as "retry once", not as a broken update or
a reason to reach for `--confirm-reinstall`.

If the source clone has local modifications or `--ff-only` fails, report it
and skip that tool rather than resolving on your own.

After updating, re-read each metadata file to confirm the new version and show
a summary of what changed (`git status --short`).

### 5. Land the update (default)

A confirmed tool bump is a safe, reversible, version-controlled change (the
installer/updater is idempotent and re-runnable), so by default `update-tools`
**commits it and lands it on the default branch** rather than stopping at an
uncommitted diff. It **never** pushes — pushing is outward-facing and stays a separate,
explicit action (Safety Rule 5). Pass `--no-commit` (alias `--stage-only`) to
skip this step and leave the working-tree changes uncommitted for manual review
instead (the old behavior).

Land each tool's bump as its own commit:

1. **Isolate the installer's footprint.** Snapshot the working tree *before*
   running the installer so a pre-existing dirty tree is never folded into the
   update commit. **Use a literal, spelled-out scratch path — never a
   `$(mktemp)`-assigned shell variable — as the `>` redirect target.** In a
   Loom-managed repo the destructive-write guard denies a write whose target
   is an unexpanded shell variable outright, because it cannot statically
   resolve where the write lands (fail-closed, #4921/#4178); a literal path
   sidesteps that ambiguity entirely, so do not "clean this back up" into
   `$pre`/`$post`/`$changed` variables for readability. Prefer the session's
   own scratchpad directory when the agent has one; otherwise spell out a
   `/tmp/...` path directly:

   ```bash
   git -C <this-repo> status --porcelain | sed 's/^...//' | sort > /tmp/update-tools-pre.txt
   # ... run the tool's installer (step 4, above) ...
   git -C <this-repo> status --porcelain | sed 's/^...//' | sort > /tmp/update-tools-post.txt
   # Paths the installer actually changed = post minus pre:
   comm -13 /tmp/update-tools-pre.txt /tmp/update-tools-post.txt > /tmp/update-tools-changed.txt
   ```

   Stage **only** those paths
   (`git -C <this-repo> add -- $(cat /tmp/update-tools-changed.txt)`), never
   `git add -A`. If `/tmp/update-tools-changed.txt` is empty the installer was
   a no-op — report "already current" and skip the commit for that tool.

2. **Commit + land on the default branch, without committing straight to it:**

   ```bash
   DEFAULT=$(git -C <this-repo> symbolic-ref --quiet --short refs/remotes/origin/HEAD | sed 's#^origin/##')
   DEFAULT=${DEFAULT:-main}
   CUR=$(git -C <this-repo> symbolic-ref --short HEAD)
   MSG="chore(tooling): update <tool> <old>→<new>"
   # Commit-drift update (version identical on both sides): <old>→<new> would
   # read "0.9.0→0.9.0" and say nothing. Use the installed commits instead:
   #   chore(tooling): update <tool> 0.9.0 (abc1234→def5678)

   if [ "$CUR" = "$DEFAULT" ]; then
     # On the default branch: commit on a short-lived branch, then fast-forward
     # merge it in — lands on the default branch without a straight-to-main commit.
     tmp="tooling/update-<tool>-<new>"
     git -C <this-repo> checkout -b "$tmp"
     git -C <this-repo> commit -m "$MSG"
     git -C <this-repo> checkout "$DEFAULT"
     git -C <this-repo> merge --ff-only "$tmp"
     git -C <this-repo> branch -d "$tmp"
   else
     # Already on a feature branch: commit here and report where it landed —
     # do NOT switch branches mid-session and disturb the user's working state.
     git -C <this-repo> commit -m "$MSG"
     echo "Landed the update on '$CUR' (not '$DEFAULT') — you are on a feature branch."
   fi
   ```

3. **Report** the resulting commit (`git -C <this-repo> log --oneline -1`) and
   remind the user it has **not** been pushed (run `git push` explicitly to
   share it).

## Safety Rules

1. **Never update without confirmation** — show installed → latest per tool
   first, including commit-drift tools, whose prompt shows installed commit →
   source `origin/HEAD` rather than a version bump
2. **Always use the tool's own installer or update mechanism** — where a tool
   ships a dedicated non-destructive updater (e.g. Loom's
   `.loom/scripts/resync-installed.sh`), prefer it over re-running the
   installer; installer reinstall is the destructive fallback, not the default
   update path. Either way, never hand-copy files — the installer/updater owns
   the write footprint and marker blocks, and hand-copying breaks reinstall
   idempotency
3. **Never resolve source-repo git problems silently** (diverged clone, dirty
   tree) — report and skip
4. **Land the update, don't just stage it** — by default commit the installer's
   changes and land them on the default branch with a per-tool
   `chore(tooling): update <tool> <old>→<new>` message. Stage **only** the paths
   the installer actually changed — never fold a pre-existing dirty working tree
   into the update commit. `--no-commit` / `--stage-only` restores the old
   leave-it-uncommitted-for-review behavior.
5. **Never push** — landing on the local default branch is reversible; pushing is
   outward-facing and stays a separate, explicit action the user runs themselves.
