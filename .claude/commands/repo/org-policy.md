---
name: "org-policy"
description: "Preview or install canonical rjwalters/repo preferences into a client's GitHub owner/.github repository"
domain: repo
type: command
user-invocable: true
---

# /repo:org-policy — Organization Preferences

Manage an organization's policy from any of its client repositories. The
authoritative files live in **rjwalters/repo**, on its default branch:

- `policies/default.json`: shared defaults.
- `policies/organizations/<lowercase-owner>.json`: optional owner overrides.

Objects merge recursively; arrays and scalar values replace the shared value.
Use the same convention for a personal GitHub account. Change preferences in
the canonical source, then redeploy; organization copies and client installs
are not independent sources of truth.

## Usage

```text
/repo:org-policy                     # Preview source, effective policy, and org drift
/repo:org-policy --check             # Same read-only report
/repo:org-policy --install           # Preview, then publish the authorized policy PR
/repo:org-policy --install --repo OWNER/PROJECT
```

The deterministic implementation is `scripts/repo/repo-org-policy.py` in the
source checkout, installed to `.claude/skills/repo/scripts/repo-org-policy.py`.
It needs Python 3.9+, git, and authenticated `gh`. It always uses github.com;
other forges are unsupported. A local source checkout is not required on the
client: the helper fetches canonical JSON at one immutable GitHub commit.

## Preview and publish

1. Derive the client owner from `origin`, or use an explicitly supplied
   `--repo OWNER/PROJECT`. Display the target **OWNER/.github**: this operation
   affects shared organization policy, not just the invoking repository.
2. Run the helper's `plan` command. For `--check`, omit `--output` and stop after
   the report. An absent/inaccessible target is not proof that creation is
   needed; verify access first. If creating `.github` is intended, establish
   public/private visibility and use `--create-repository public|private`.
   Shared policy contains no credentials. A public preset is easiest to reuse
   across public and private clients; private presets require appropriate app
   access and cannot be consumed from public repositories.
3. For an install, save a plan at a new temporary path. Show its complete diff,
   source revision, target, and repository creation/visibility if applicable.
   Use existing authorization when it covers that target and these changes;
   otherwise request approval of this concrete plan. Do not repeatedly ask for
   steps already authorized. `--install` never authorizes unrelated settings,
   app installation, or migrating every client repository.
4. Apply that saved plan with `--yes`. It rechecks source and target before any
   mutation. A changed source or target requires a fresh preview. It preserves
   unrelated files, creates a separate branch, and opens a policy PR. A retry
   reuses an unchanged existing branch/PR; it never force-pushes. If repository
   creation succeeded but publishing failed, report that partial result and
   generate a fresh plan before continuing.
5. Report the PR URL and whether it is merged. The policy becomes active on
   the default branch after merge; opening a PR is not an installed policy.
   Follow the repository's normal merge workflow when merging is authorized.

```bash
# Run from the client repository. Use a new directory so plan.json is absent.
policy_preview_dir=$(mktemp -d)
python3 .claude/skills/repo/scripts/repo-org-policy.py plan \
  --output "$policy_preview_dir/plan.json"
# After reviewing the plan, within the authorized install scope:
python3 .claude/skills/repo/scripts/repo-org-policy.py apply \
  --plan "$policy_preview_dir/plan.json" --yes
```

`--source-dir /path/to/repo` on `plan` previews unpublished canonical changes.
Those plans cannot be applied: publish the source and regenerate from GitHub.
Do not use a stale client-bundled template as a fallback when GitHub is
unavailable. Authentication/rate-limit failures are errors, not missing policy.

## Installed files and client adoption

The organization PR manages exactly two root files in `OWNER/.github`:

- `renovate-config.json`: resolved native Renovate configuration.
- `repo-policy.json`: provider/settings preferences, the preset reference, and
  source repository, commit, and source-file SHA-256 digests.

The helper does **not** install the Renovate GitHub App, change repository flags,
enable auto-merge, or migrate client repositories. After the organization PR
merges, use [[deps]] to reconcile the invoking client. Its `renovate.json`
extends `github>OWNER/.github:renovate-config`; explicit client exceptions stay
in that client. Existing clients must adopt that reference once. Future
organization policy updates then reach those clients when Renovate resolves
the preset, without reinstalling Repo Skills.

Check for an existing `OWNER/renovate-config/default.json` before relying on
automatic onboarding: Renovate discovers it ahead of `.github` presets. Use
the explicit reference above for uniform client adoption. If the helper finds
different organization content, the preview shows that drift; do not treat a
hand edit to an installed copy as a new canonical preference.

## Dependency policy

The shipped policy requests 14 days for routine versions and one day for
advisory-backed security fixes. Both ages are measured from release time.
Keep that classification separate from patch/minor/major SemVer and from
claims in a changelog. A claim alone requires triage before expedited handling.
Native Renovate security behavior defaults to no release-age delay, so the
one-day override is explicit in `vulnerabilityAlerts.minimumReleaseAge`; verify
the deployed bot honors it during onboarding, including lockfile resolution.

Automerge is a separate preference and defaults to false. Before enabling it,
verify effective required checks actually cover the changed ecosystem. An
emergency zero-delay exception must identify the affected advisory/dependency
and have explicit authorization. A targeted manual fix PR can bypass the age
hold for that dependency without shortening the organization's general policy;
verify lockfile/package-manager age exceptions as part of that PR. Remove any
temporary client exception afterward. It does not waive CI or breaking-change
review.

For source layout, override examples, and validation, read `policies/README.md`
in rjwalters/repo. Native reference:
https://docs.renovatebot.com/config-presets/#grouporganization-level-presets
