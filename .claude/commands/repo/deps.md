---
name: "deps"
description: "Third-party dependency currency — reconcile organization policy, Renovate or Dependabot setup, and bot PRs; report-only under --check"
domain: repo
type: command
user-invocable: true
---

# /repo:deps — Third-Party Dependency Currency

Keep the repo's **third-party dependencies** current: npm / pip / cargo / Go
packages and GitHub Actions. Two halves, usually run together:

1. **Resolve organization policy and verify the updater** — Renovate or
   Dependabot, independently of GitHub vulnerability alerts and security PRs.
2. **Triage dependency PRs** — actual changes, age eligibility, security
   relevance, CI coverage, and merge policy.

This is the companion to [[update-tools]], not a part of it. `update-tools`
compares *installer-managed tool packages* (Loom, Anvil, Repo Skills) against a
local source clone; there is no source clone to diff for Dependabot, and
"triage incoming bot PRs" is a different activity from "update an installed
package." Keeping them separate keeps `update-tools`' comparison model intact.

Everything here either writes repo config, flips a repository setting, or
merges a PR — so like `release`, `remote`, `followups`, and `update-tools`,
this command requires authorization for writes and merges. Apply existing
authorization when it covers the concrete changes; otherwise show the changes
and confirm first. `--check` is always report-only.

## Usage

```
/repo:deps                  # Report policy, updater status, and open dependency PRs
/repo:deps --check          # Report only — never writes, never merges
/repo:deps --install        # Only setup reconciliation (policy + config + settings)
/repo:deps --review         # Only the PR-triage half
/repo:deps --review 123     # Triage one PR in depth
```

## Prerequisites

This command currently supports GitHub. Confirm the repo is on GitHub before doing
anything else — if `origin` points at Gitea or another forge, say so and stop
rather than scaffolding config that will never run:

```bash
git config --get remote.origin.url    # → derive OWNER/REPO; must be a GitHub host
gh auth status
```

## 0. Resolve policy and choose the provider

Read root `repo-policy.json` from `OWNER/.github` on its default branch, if
accessible. This is the deployed organization policy. It records the canonical
source in **rjwalters/repo/policies/**, its revision, desired provider/settings,
and `dependencies.renovatePreset`. [[org-policy]] compares that installed copy
with the canonical source and prepares organization updates. Do not invent a
second organization preference file in a client or change an installed org copy
as if it were authoritative. An absent file, access failure, and invalid policy
are distinct states; do not silently fall back after a permissions/network or
validation error.

Also detect existing Renovate config, including `renovate.json`, `renovate.json5`,
`.github/renovate.json[5]`, `.renovaterc[.json]`, `.renovaterc.json5`,
`.gitlab/renovate.json[5]`, and the `renovate` key in `package.json`, alongside
Dependabot config. Inspect Renovate's current supported config locations if
none of these explains an active installation. Preserve an existing provider
unless migration is authorized. With no provider or organization policy, offer
[[org-policy]] before proposing new automation; do not silently assume that
missing `dependabot.yml` means dependencies are unmanaged.

Report separately: deployed policy source/revision and drift, provider/config,
app operation, dependency graph, vulnerability alerts, security PR provider,
effective release-age rules, automerge/required checks, and open bot PRs.
`dependabotSecurityUpdates: false` is expected when Renovate owns security PRs;
it is not a finding that should be "fixed" by enabling a second provider.

### Renovate setup path

When Renovate is selected, use this path instead of Dependabot steps 1, 4–6.
Reuse steps 2–3 below for ecosystem/ownership detection and label validation.

- Confirm the organization policy PR is merged and its preset is readable by
  the Renovate installation. Config presence alone does not prove an active
  GitHub App or a successful scan: inspect onboarding, the Dependency Dashboard,
  and job logs. App installation may require an organization administrator.
- Update the existing active Renovate config (do not add a competing file) to
  extend the deployed `dependencies.renovatePreset`, usually
  `github>OWNER/.github:renovate-config`. Preserve intentional client exceptions
  and report any override that changes age or merge policy. Use a minimal
  `renovate.json` with that `extends` entry only when no config exists.
- Exclude the installer-owned roots discovered in step 2a through `ignorePaths`,
  preserving existing exclusions. Skip dependency-free manifests. Validate the
  resulting effective native config with `renovate-config-validator`.
- Keep the dependency graph and Dependabot alerts enabled and grant Renovate
  read access to alerts. Probe the dedicated alerts and security-fixes endpoints
  shown in step 1. Treat `dependencies.dependabotSecurityUpdates` as desired
  state: disable Dependabot PR generation only after Renovate's fix coverage is
  verified for this client's ecosystems and lockfiles. Report pending migration
  instead of temporarily removing all security-fix automation.
- During migration, remove/disable the overlapping Dependabot version-update
  entries once Renovate is operational. Retain any deliberately assigned
  Dependabot-only coverage and inspect old PRs individually; do not bulk-close
  them just because the provider changed.
- Verify release timestamps and package-manager age controls, including new
  transitive versions during lockfile generation. The baseline is 14 days for
  routine versions, one day for advisory-backed security fixes. Check that the
  explicit security age override is honored by the deployed bot; its default
  security behavior is zero delay. A changelog claim or patch version number is
  insufficient to automatically grant the security exception. An emergency
  override requires the affected advisory/dependency and explicit authorization;
  scope it to this client and remove it afterward.
- Eligibility and merging are separate. The shared baseline leaves automerge
  off. Before opting in, verify required checks cover the affected ecosystems;
  never bypass CI or assume all Actions majors are low-risk. Do not attach
  reserved Loom labels to route bot PRs into another merge pipeline.

Show concrete config/settings changes before applying authorized setup. Continue
with the common PR review below. Under `--check`, make no changes, including
organization deployments, client files, flags, or merges.

## Steps — Dependabot install / verify

Use this path when Dependabot remains the chosen provider. An organization's
Renovate policy is a migration proposal until adoption is authorized, not a
reason to silently replace a working Dependabot installation.

### 1. Report config and the security flag as two distinct items

Writing `.github/dependabot.yml` enables **version** updates only. Dependabot
**security** updates are a repository setting that is entirely independent — a
repo can have a perfectly good config file and still have CVE alerting off.
Check and report both:

```bash
git ls-files '.github/dependabot.yml' '.github/dependabot.yaml'   # version updates

# Security updates — a dedicated endpoint, NOT a security_and_analysis key:
# returns a definitive {"enabled": bool}; 403 → needs admin (see UNKNOWN note below)
gh api repos/OWNER/REPO/automated-security-fixes --jq '.enabled'

# Alerts — likewise a dedicated endpoint, NOT a security_and_analysis key:
#   204 → enabled, 404 → disabled
gh api repos/OWNER/REPO/vulnerability-alerts -i 2>/dev/null | head -1
```

Read both flags from their dedicated endpoints, never from `security_and_analysis`.
That object is an unreliable source for either one, for two different reasons:

- `security_and_analysis.dependabot_alerts` is simply **absent** on many repos
  even when the object is otherwise fully populated, so a `// "UNKNOWN"`
  fallback on it reports "can't tell" for a repo you can read perfectly well.
  (Verified against `rjwalters/repo`: `security_and_analysis` returns
  `dependabot_security_updates` and the `secret_scanning*` keys with no
  `dependabot_alerts` among them.)
- On a **private repo without GitHub Advanced Security**, GitHub omits the
  **whole `security_and_analysis` object** regardless of token permissions —
  even a token with full admin sees it absent. So "object absent" and "no
  permission to see it" are different states, and only `automated-security-fixes`
  returning `403` is evidence of the latter; treating an absent
  `security_and_analysis` object itself as proof of missing admin is not
  reliable and misreports a plan/visibility limitation as a permission gap.

Report them on separate rows, never collapsed into one "Dependabot: on":

```
DEPENDABOT
==========
| Item                            | Status                                  |
|---------------------------------|-----------------------------------------|
| .github/dependabot.yml          | absent — no version updates configured  |
| vulnerability alerts (repo flag)| disabled (404)                          |
| security updates (repo flag)    | disabled — no automatic CVE fix PRs     |
| Open Dependabot PRs             | 0                                       |
```

Reserve **UNKNOWN (needs admin)** for an actual permission failure — a `403`
from `/automated-security-fixes` (security updates) or `/vulnerability-alerts`
(alerts). Do **not** report a flag as `disabled` when the endpoint returned
`403`; "can't see it" and "it's off" are different answers and only one of
them justifies a write. Conversely, do **not** infer UNKNOWN from an absent
`security_and_analysis` object — as noted above, private repos without GitHub
Advanced Security omit that object even for a fully-admin token, so its
absence alone proves nothing about permissions; the dedicated endpoints are
the authoritative source either way.

### 2. Detect the ecosystems actually present

Scaffold from what the repo really contains, never from a fixed template. Look
for manifests at the root **and** in subdirectories (every distinct directory
needs coverage — either its own `updates:` entry with the right `directory:`
value, or a slot in one entry's plural `directories:` list, see below):

| Ecosystem | Detect via |
|---|---|
| `github-actions` | `.github/workflows/*.yml`, `.github/actions/*/action.yml` |
| `npm` | `package.json` (`pnpm-lock.yaml` / `yarn.lock` / `package-lock.json`) |
| `cargo` | `Cargo.toml` |
| `pip` | `requirements*.txt`, `pyproject.toml`, `Pipfile` |
| `gomod` | `go.mod` |
| `bundler` | `Gemfile` |
| `composer` | `composer.json` |
| `docker` | `Dockerfile`, `docker-compose.yml` |
| `gitsubmodule` | `.gitmodules` |

```bash
git ls-files '.github/workflows/*' '.github/actions/*' \
  '*package.json' '*Cargo.toml' '*go.mod' '*requirements*.txt' '*pyproject.toml' \
  '*Pipfile' '*Gemfile' '*composer.json' '.gitmodules' '*Dockerfile' \
  '*docker-compose.yml'
```

Use `git ls-files` for **every** probe — including the workflow directory — not
`ls` with a glob. Two reasons: vendored and ignored trees can't produce phantom
ecosystems, and under `zsh` an unmatched glob like `ls .github/workflows/*.yaml`
is a **hard error that aborts the whole command line**, so a repo with `.yml`
workflows can end up reporting no Actions ecosystem at all. `2>/dev/null` does
not save you — zsh fails before `ls` ever runs.

If **nothing** is detected, say there is nothing to scaffold; do not guess an
ecosystem. Still report alerts/settings through the selected provider's path.

**One ecosystem in many directories**: when the same ecosystem appears in
several places (say, four independent crates, each with its own `Cargo.toml`
and `Cargo.lock`, under one directory tree), Dependabot's plural `directories:`
key collapses them into a **single** `updates:` entry instead of N
near-identical ones. Scans still open one PR per directory — the key changes
how the config is written, not how many PRs arrive. The trade-off is that one
entry means **one shared policy**: the same schedule, grouping, `labels:`, and
`exclude-patterns` apply to every listed directory. Keep separate
per-directory entries whenever two manifests genuinely need different grouping
or cadence. Syntax in step 4.

#### 2a. Classify each manifest as repo-owned or installer-owned

Presence alone is not ownership. A manifest that lives under a tool root
installed by Loom/Anvil/Repo-Skills-style installers is *vendored,
installer-owned* code — the next tool install/upgrade overwrites it, so a
Dependabot PR against it is churn, not value. This is the same signal
[[update-tools]] step 1 already sweeps for — use the same bounded `find`, not a
fixed path list, so a future tool family member is picked up without a doc
edit here too:

```bash
find . -maxdepth 4 -name "install-metadata.json" \
  -not -path "*/node_modules/*" -not -path "*/.venv/*" 2>/dev/null
```

Each hit establishes a **tool root** (the directory containing that
`install-metadata.json`, e.g. `.anvil/`, `.loom/`, `.kct/`,
`.claude/skills/repo/`). A manifest from step 2 is **installer-owned** when its
path falls under one of these roots; everything else is **repo-owned**.

Do this classification per manifest, not per ecosystem — an ecosystem can have
both a repo-owned and an installer-owned manifest at once (e.g. a root
`pyproject.toml` alongside `.anvil/pyproject.toml`), and only the latter is
excluded.

Report the two groups separately, and never fold an installer-owned manifest
into the scaffold proposal:

```
MANIFESTS
=========
| Manifest                | Ecosystem | Ownership                                        |
|--------------------------|-----------|---------------------------------------------------|
| pyproject.toml (root)   | pip       | repo-owned                                       |
| .anvil/pyproject.toml   | pip       | installer-owned (anvil); use /repo:update-tools  |
| .anvil/uv.lock          | pip       | installer-owned (anvil); use /repo:update-tools  |
| package.json (root)     | npm       | repo-owned — but dependency-free (see below)     |
```

A dependency-free manifest is also not scaffoldable: a `package.json` with no
entries in `dependencies`, `devDependencies`, `peerDependencies`, or
`optionalDependencies`, and no lockfile, has nothing for Dependabot to update.
Check it explicitly rather than assuming presence implies content:

```bash
jq '{dependencies, devDependencies, peerDependencies, optionalDependencies}' package.json
```

If every detected manifest for an ecosystem is either installer-owned or
dependency-free, that ecosystem drops out of the scaffold candidate list
entirely — it does not get an `updates:` entry.

If, after this filtering, **no ecosystem remains** — every detected manifest
was installer-owned, dependency-free, or both — do not propose a
`dependabot.yml` at all. Say so explicitly and why, and point installer-owned
findings at `/repo:update-tools` as the remediation path for their
dependencies (that command upgrades the vendored manifest itself; a Dependabot
PR against it would just be reverted by the next install). Still continue to
the selected provider's repo-level alert/settings checks — those are useful
independent of whether there is anything to scaffold. Use step 5 only on the
Dependabot path; Renovate must not re-enable duplicate security PRs.

### 3. Validate every label the config would reference — by description

A scaffolded config can attach labels to bot PRs (`labels:` in the `updates:`
entry). Before referencing **any** label, read its description and confirm a
bot may apply it:

```bash
# Labels reserved for a specific party — refuse every one of these, and name
# the party in the report
gh api repos/OWNER/REPO/labels --paginate \
  --jq '.[] | select(.description // "" | test("Applied by:")) | "REFUSE: \(.name) — reserved for \(((.description // "") | capture("Applied by: (?<party>[^.]+)").party // "unknown party") | sub(" only\\s*$"; "")) — \(.description)"'

# Remaining candidates
gh api repos/OWNER/REPO/labels --paginate \
  --jq '.[] | select((.description // "" | test("Applied by:")) | not) | .name'
```

Two details that matter in that jq: `.description // ""` is **required** — a
label with a null description makes a bare `.description | test(…)` abort with
`null (null) cannot be matched, as it is not a string`, which can drop the rest
of the label list mid-scan and silently shrink the set you validate against.
And prefer `gh api …/labels` over `gh label list --json` — the latter goes
through GraphQL, which shares a separate (and, on a busy agent host, routinely
exhausted) rate-limit bucket from REST.

Rules:

- The label must **exist**. Existence alone is not enough.
- **Refuse any label whose description reserves it for a party** — look for
  the literal substring `Applied by:`, not just `Applied by: humans`. The
  convention reserves labels for parties other than humans too — e.g.
  `loom:evaluating`: *"Champion is evaluating this proposal (claim label,
  stale after 15m). Applied by: Champion only."* is exactly as off-limits to
  Dependabot as a human-reserved label: it is a claim label with staleness
  semantics owned by a specific actor, and Dependabot applying it would feed
  automation that acts on that claim. Having Dependabot apply *any*
  `Applied by:` label violates that label's own contract, regardless of which
  party it names.
- Report which party each refused label is reserved for (e.g. "REFUSE:
  loom:evaluating — reserved for Champion").
- **Never create a label** to solve this. No `gh label create`, ever. If no
  suitable label exists, scaffold the config **without** a `labels:` key and
  say so in the report. This refuse-by-default posture is intentionally
  stricter than necessary — it can pass over a label that a repo owner would
  in fact consider fine for Dependabot to apply (e.g. an `Applied by: <bot>`
  label meant for automation) — but the safe fallback of no `labels:` key
  costs nothing, while silently applying a reserved label can violate its
  contract. If a repo wants a bot-applied label used here, that is a policy
  call for a human to make explicitly, not something this check should infer.
- A maintenance/chore-tier label (e.g. `tier:maintenance`, `dependencies`,
  `chore`) is the usual right answer when one is present and unrestricted.

Report the decision explicitly: which label was chosen, or which were rejected
and why.

### 4. Offer to scaffold the config (confirm first)

Only ecosystems that survived step 2a's filtering — repo-owned manifests with
real dependencies — are candidates here. If step 2a already concluded there is
nothing left to scaffold, skip straight to step 5 rather than proposing a
config anyway.

Grouping policy is **per-ecosystem**, not uniform. Reviewing every Actions bump
individually is noise; batching a breaking change into line 4 of a 12-package
PR is how a risk-bearing dependency slips through unreviewed.

| Ecosystem | Policy | Why |
|---|---|---|
| `github-actions` | group everything, majors included | low-risk, individually reviewing them is noise |
| package ecosystems (`npm`, `cargo`, `pip`, …) | group minor + patch; **majors ungrouped** | a breaking change deserves its own reviewable PR |

**Ask which dependencies are risk-bearing** rather than applying one policy to
everything — deps coupled to an external binary or service (e.g.
`playwright-core` and its browser binary, a database driver, a native
toolchain) should stay ungrouped even at minor/patch, via `exclude-patterns`.

Show the proposed file in full and get approval before writing:

```yaml
version: 2
updates:
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
    groups:
      github-actions:
        patterns: ["*"]
        update-types: ["major", "minor", "patch"]

  - package-ecosystem: "npm"
    directory: "/"
    schedule:
      interval: "weekly"
    groups:
      npm-minor-patch:
        patterns: ["*"]
        exclude-patterns: ["playwright-core"]   # risk-bearing → its own PR
        update-types: ["minor", "patch"]
    # majors are deliberately ungrouped: one reviewable PR each
```

For an ecosystem step 2 found in several directories, use the plural
`directories:` key **in place of** `directory:` — a list of paths (globs
allowed) sharing one entry's schedule, grouping, and labels:

```yaml
  - package-ecosystem: "cargo"
    directories:                       # plural — several manifests, one policy
      - "/crates/alpha"
      - "/crates/beta"
      - "/crates/gamma"
    schedule:
      interval: "weekly"
    groups:
      cargo-minor-patch:
        patterns: ["*"]
        update-types: ["minor", "patch"]
```

Expect one PR per directory on the first scan even though there is only one
entry — `directories:` shares the *policy*, not the PRs. Split it back into
per-directory entries the moment two of those crates need different grouping,
cadence, or labels: `directories:` buys brevity, and pays for it with a single
shared policy across every path listed.

Add `labels: ["<validated-label>"]` only if step 3 approved one. Write the file
only on explicit approval; under `--check`, stop here and show it as a proposal.

### 5. Offer to enable the repo-level flags (confirm first)

Independent of the file, and a separate confirmation. Alerts are a
**prerequisite** for automated security fixes — enable in this order:

```bash
gh api -X PUT repos/OWNER/REPO/vulnerability-alerts       # prerequisite
gh api -X PUT repos/OWNER/REPO/automated-security-fixes
```

Both need admin. On a 403, report that the flag needs a repo admin and move on
— never present a failed write as success. Re-read the flags afterward and show
the before/after.

### 6. Check for PRs immediately after the config lands

**Dependabot fires on config merge, not on schedule.** The first PRs typically
arrive within a couple of minutes of the config landing on the default branch,
regardless of `interval: weekly`. Never tell the user to "expect your first PR
Monday" — wait briefly, then run the PR review half below.

## Steps — review open dependency PRs

### 7. List the selected providers' open PRs

For hosted Renovate include `renovate[bot]` (CLI author `app/renovate`);
for self-hosted installations discover the configured bot identity rather than
assuming that login. During migration include both providers, verifying authors
against the actual installation. A `renovate/` branch name alone proves no bot
identity. The examples below show Dependabot; extend the author filter for the
providers actually present.

```bash
gh pr list --author "app/dependabot" --state open \
  --json number,title,headRefName,createdAt,statusCheckRollup,labels

# REST fallback when GraphQL's rate-limit bucket is exhausted. Note the author
# spelling differs: gh's --author filter wants "app/dependabot", the REST
# payload carries login "dependabot[bot]". This jq deliberately emits only
# number/title — if a later step starts consuming headRefName/createdAt/
# statusCheckRollup/labels, widen it, or the fallback path silently loses them
# (statusCheckRollup has no REST field: use `gh pr checks <N>` per PR instead).
gh api repos/OWNER/REPO/pulls --paginate \
  --jq '.[] | select(.user.login == "dependabot[bot]") | "#\(.number) \(.title)"'
```

If there are none, say so — and if the config was just written, note that PRs
land within minutes rather than on the stated interval.

### 8. Classify each PR

For every PR report: **ecosystem**, **update type** (major vs minor/patch —
majors flagged), **CI status**, **whether it's stale** (the manifest on the
base branch already satisfies it — see the sub-step below), and what actually
changed.

Update type comes from the title/branch (`bump X from 1.2.3 to 2.0.0` →
compare the leading version components) — confirm against the diff rather than
trusting the title alone:

```bash
# REST rather than `gh pr view --json` — same reason as the label lookup above:
# any `--json` flag on `gh pr`/`gh issue` forces a GraphQL query. `gh pr diff`
# and `gh pr checks` take no `--json` and are already REST-backed.
gh api repos/OWNER/REPO/pulls/<N> --jq '{title, body}'
gh api repos/OWNER/REPO/pulls/<N>/files --paginate --jq '[.[].filename]'
gh pr diff <N>
gh pr checks <N>
```

#### CI status caveat — green only means what CI actually exercises

**Scaffolding an ecosystem's Dependabot config does not imply the repo's CI
exercises that ecosystem's artifact.** `/repo:deps` scaffolds an ecosystem
purely from manifest presence (Safety Rule 4) — it never checks whether any CI
job builds or runs what that manifest produces. A bot PR's "CI status" is only
as meaningful as what CI does with the changed files: a `npm`/`cargo`/`pip`
bump that CI compiles and tests is well-verified by green; a `docker`-ecosystem
bump (base-image or layer change) is verified by green only if some job
actually runs `docker build`. This generalizes to any ecosystem whose
artifact CI neither builds nor runs — Terraform providers with no `terraform
plan` job, a Helm chart with no `helm template`/lint step, etc.

Before reporting a PR's CI status as reassuring, check whether the repo's
workflows actually build/run that ecosystem's artifact (`grep` the
`.github/workflows/*.yml` for the relevant command — `docker build`,
`terraform plan`, …). If they don't, report the PR's CI as **green but
unverifiable by this repo's checks** rather than plain "green" — e.g. a
`docker`-ecosystem PR when no workflow runs `docker build` against the
Dockerfile it touches. (As of this writing this repo's own `docker` entry —
the root `Dockerfile` — is covered: `.github/workflows/docker-build.yml` runs
`docker build .` on any PR/push touching it, `paths:`-filtered off unrelated
PRs — added in response to issue #231, after the first docker Dependabot PR
merged on a green check that had built nothing. Re-check this note against
the workflow files each run rather than trusting this parenthetical, since
either side of it can drift.)

#### Stale check — compare actual dependency state on the current base

Inspect every targeted manifest **and lockfile** from the current remote base
commit. A manifest range permitting the proposed version does not prove the
installed dependency already uses it: `^1.0.0` can still lock a vulnerable
`1.0.0` while a security PR changes only the lockfile to `1.0.1`.

A PR is stale only when every proposed change is already present or superseded
on the base. Use the ecosystem's version/range semantics, not lexical ordering
or a generic comparison of leading digits. For grouped/workspace updates check
all affected manifests and lockfiles. For security fixes also confirm the
resolved version is outside the advisory's affected range; a higher version
can still be vulnerable. Check digest changes independently of version tags.

For GitHub Actions, compare the pins in every workflow/action file the PR
changes. Two PRs with the same version pair can update different workflows;
a matching title never establishes duplication. Treat ambiguous or partially
satisfied PRs as pending work, not stale, and do not close them automatically.

Exclude truly stale PRs from the pending-major tally, but retain their separate
count and evidence in the report.

For **GitHub Actions** bumps specifically, check whether the update **clears a
deprecation annotation** — often the actual reason to take a scary-looking
major. Compare annotations on the base branch against the PR head:

```bash
gh api "repos/OWNER/REPO/commits/$SHA/check-runs" --jq '.check_runs[].id' \
  | while read -r id; do
      gh api "repos/OWNER/REPO/check-runs/$id/annotations" --jq '.[].message'
    done
```

Run it for `main`'s head SHA and the PR's head SHA and diff the two sets. A
major bump that removes a *"Node.js 20 is deprecated"* annotation, with CI
green on every matrix leg, is a much easier yes than "a major bump, seems
risky."

The `Note` column carries the `stale — already satisfied by manifest` flag
alongside the existing CI-status/diff notes, so a stale PR is visible as such at
a glance:

```
OPEN DEPENDENCY PRs
===================
| PR  | Ecosystem      | Update                     | Type  | CI    | Note                              |
|-----|----------------|----------------------------|-------|-------|-----------------------------------|
| #12 | github-actions | actions/checkout 4 → 5     | MAJOR | green | clears "Node.js 20 deprecated"    |
| #13 | npm            | 6 packages (minor + patch) | minor | green | grouped                           |
| #14 | npm            | playwright-core 1.4 → 2.0  | MAJOR | red   | browser binary coupling           |
| #15 | npm            | @biomejs/biome 2.5.5 → 2.5.6 | patch | green | stale — base lockfile already resolves 2.5.7 |
```

Summarize the split explicitly below the table so callers (including
`/repo:all`) get the counts without re-deriving them —
**open**, **majors** (real forward majors only), and **stale**:

```
4 open, 2 majors, 1 stale — already satisfied by manifest
```

The majors count excludes every stale PR. A PR whose title names a major bump
but whose resolved dependency state already satisfies it (stale) is **not** a major here — it
is counted only in the stale total.

### 9. Offer to merge the safe ones (confirm first)

Propose a merge set and get explicit approval. **Never** merge a major without
its own separate confirmation, and never merge a PR whose CI is red or pending.

In a Loom-managed repo (`.loom/scripts/merge-pr.sh` present) use that script
rather than `gh pr merge` — `gh pr merge` attempts a local checkout that fails
when the branch is linked to a worktree:

```bash
./.loom/scripts/merge-pr.sh <N>      # Loom repos
gh pr merge <N> --squash             # otherwise
```

**Re-poll mergeability between sequential merges.** Merging one bot PR that
touches a shared lockfile invalidates its siblings: GitHub recomputes their
mergeability asynchronously, and for ~20 s the remaining PRs report
`mergeable: null` / `mergeable_state: "unknown"` (`mergeStateStatus: UNKNOWN`
via `gh`) before settling back to `clean`. `merge-pr.sh` refuses inside that
window, which reads as a spurious failure mid-loop. After each merge, re-read
the next PR's state and wait for `clean` before merging it:

```bash
for _ in $(seq 1 12); do
  state=$(gh pr view <N> --json mergeStateStatus -q .mergeStateStatus)
  [ "$state" = "UNKNOWN" ] || break
  sleep 5
done
echo "$state"   # CLEAN → merge; DIRTY/BLOCKED → real conflict or failing checks, re-triage
```

A state that settles on `DIRTY` (or `BLOCKED`) is **not** a timing artifact —
the earlier merge produced a genuine lockfile conflict, so stop the loop and
report it rather than retrying.

Under `--check`, stop at the report and merge nothing.

## Check how dependency PRs interact with Loom

State this in the report whenever a `.loom/` directory is present. It is the
natural wrong assumption, and it is safety-relevant:

- Inspect the actual bot PR labels and installed Loom routing. Do not assume
  either Dependabot or Renovate PRs are watched or ignored based on bot identity
  alone; unlabeled PR handling can vary with the installed Loom version.
- Report who is responsible for merging: a human, the configured updater, or an
  explicitly authorized Loom workflow. Eligibility is not merge authorization.
- Do **not** "fix" this by applying `loom:` labels to bot PRs. Routing bot PRs
  into an auto-merge pipeline is a policy decision for the repo's owner, not a
  side effect of a hygiene command — and any label used for it still has to
  pass the step 3 description check.

## Safety Rules

1. **Writes and merges require authorization** — show the concrete scope and
   use existing authorization when it covers it. Under `--check`, write nothing.
2. **Policy, updater, alerts, and security PRs are independent** — a present
   config says nothing about whether CVE alerting is on. Report
   UNKNOWN (not `disabled`) when the token can't read the setting.
3. **Never create a label**, and never reference one whose description reserves
   it for any party (`Applied by: <party>` — humans, Champion, a bot, …). No
   suitable label → no `labels:` key.
4. **Scaffold only detected ecosystems** — no fixed template, no guessing. Zero
   detected means nothing to scaffold.
5. **Never scaffold against an installer-owned manifest** — a manifest under a
   tool root that carries `install-metadata.json` (`.anvil/`, `.loom/`,
   `.claude/skills/*/`, …) is vendored code the next tool install/upgrade
   overwrites; propose `/repo:update-tools` for it instead. Also exclude
   dependency-free manifests (no deps in any block, no lockfile) — nothing for
   Dependabot to update. If every detected ecosystem is installer-owned or
   dependency-free, recommend not scaffolding and say why.
6. **Never auto-merge a major** — majors get their own confirmation, always.
   Red or pending CI is never merged.
7. **Never push or merge under `--check`** — report-only means report-only.
