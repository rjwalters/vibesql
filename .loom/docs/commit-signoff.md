# Commit sign-off (DCO): the `commit.signoff` knob

Some repositories require a [Developer Certificate of Origin](https://developercertificate.org/)
(DCO) sign-off on **every** commit — a `Signed-off-by: Name <email>` trailer,
produced by `git commit --signoff`. These repos typically enforce it with a
required `sign-off` / `DCO` status check that fails any PR whose commits lack the
trailer.

Loom's Builder and Doctor roles author the commits that land in a PR, so a
DCO-requiring repo will fail those PRs on the first Judge pass unless the roles
sign off. This page documents the opt-in knob that makes them do so
deterministically.

## The knob

Add to the repo's `.loom/config.json`:

```json
{
  "commit": {
    "signoff": true
  }
}
```

When `commit.signoff` is `true`, the Builder and Doctor roles pass `--signoff` on
**every** commit they author, including `git commit --amend` and the rebase +
force-push path. Each resulting commit carries a `Signed-off-by:` trailer for the
committing identity.

**The knob is the load-bearing guarantee.** It is read the same way roles already
read `buildGate.command` — from `.loom/config.json`, by the role prompt. There is
no git-native "always sign off `git commit`" setting (`format.signoff` affects
`git format-patch` only), so the trailer must come from `--signoff` on each commit.

### Absent by default

`commit.signoff` is **opt-in and absent by default** — it is intentionally *not*
present in `defaults/config.json`, and enabling it is a per-consuming-repo choice.
When the knob is unset, commit behavior is unchanged (no trailer added), except for
the advisory heuristic below.

## Detection heuristic (advisory fallback)

When the knob is **unset**, the roles do one bounded, best-effort check before the
first commit and use `--signoff` if any of these fire (noting it in the PR body):

- `CONTRIBUTING.md` (or a `DCO` / `DCO.txt` file) mentions `Signed-off-by` or DCO, or
- the repo has a required status check whose name matches `dco` or `sign-?off`.

The heuristic is **advisory, not load-bearing** — set the knob for a guarantee. A
`--signoff` on a repo that does not require it is harmless: it only adds a trailer.

## Edge cases

- **No duplicate trailer**: `git commit --signoff` does not add a second identical
  `Signed-off-by:` trailer when one for the same identity is already present.
- **Amend / rebase**: the same rule applies to `git commit --amend --signoff` and
  to any commit re-authored during a rebase + `git push --force-with-lease`.
- **Existing trailer**: a commit that already carries the trailer is left as-is.

## Optional hook backstop (not installed by default)

A repo-local `prepare-commit-msg` hook that appends the trailer when missing would
make the guarantee independent of prompt adherence. Loom does **not** install one:
`worktree.sh` stays free of DCO-specific behavior, and the knob-driven prompt
guidance is the mechanism. Repos that want a hard, prompt-independent guarantee can
add such a hook themselves.
