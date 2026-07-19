# Maintenance Contract

One rule keeps this guide worth reading: **docs change in the same PR as the
code that invalidates them.** An onboarding guide that lies is worse than no
guide — a newcomer has no way to tell the true pages from the stale ones, so
one broken promise poisons all of them. The same PR, not "a follow-up",
because follow-ups don't happen and reviewers can only verify what's in front
of them. Corollary for reviewers: a checked definition-of-done box
([Adding a Module](05-adding-a-module.md)) is a *claim*, and you are entitled
to audit it like any other line of the diff.

## The trigger table

Find your change in the left column; update everything in the right column in
the same PR.

| If your PR changes… | Update… |
|---|---|
| Adds a new module | Nothing in this guide — it teaches the *pattern*, not the inventory. Do update the layout tree in `README.md`. |
| Module file layout or dependency rules (the six files, `router → business → repository`, module isolation) | [Architecture Tour](03-architecture-tour.md) + [Adding a Module](05-adding-a-module.md) |
| Any guarantee or its mechanism — inbox dedup, version guard, batcher, poison handling, UoW/publish semantics | [Reliability Model](04-reliability-model.md) + the Guarantees section of `README.md` |
| New or renamed `SDS_*` setting (`app/config/settings.py`) | [Setup](02-setup.md) config table + [Operations](07-operations.md) knobs |
| Log messages, `/health` / `/ready` shape, or shutdown order | [Operations](07-operations.md) |
| Test fixtures, fakes, or suite layout | [Testing](06-testing.md) |
| Implements the Outbox | [Reliability Model](04-reliability-model.md) gap section + the known-gap paragraph in [What & Why](01-what-and-why.md) |

If your change isn't in the table, ask the question directly: *which chapter,
if a newcomer read it tomorrow, would now teach them something false?* Update
that chapter.

## Keeping the site healthy

The site config is `mkdocs.yml` at the service root; `docs_dir` is
`onboarding/` and the built output lands in `.site/` (generated,
git-ignorable — never edit it).

```bash
.venv/bin/mkdocs build    # check: treat broken-link warnings as failures
.venv/bin/mkdocs serve    # live preview at http://127.0.0.1:8000
```

Run the build after any docs change. MkDocs reports a broken internal link as
a warning, not an error — treat it as a failure anyway; a dead link in an
onboarding guide is a wall for exactly the person least equipped to climb it.

## Related documentation

Four places hold prose about this service. Know which is which, and put new
writing where it belongs:

| Location | What it is | Who reads it |
|---|---|---|
| `README.md` | Front door: what/stack/layout/run/guarantees, one screen | Everyone, first |
| `docs/architecture.md` | Design rationale — terse, decision-dense, no tutorial | Maintainers who ask "why is it built this way?" |
| `../shared_data_service_docs/` | Original spec + acceptance checklist | Historical reference; not maintained as living docs |
| `onboarding/` (this guide) | Tutorial + runbook, rendered by MkDocs | New maintainers, managers, AI agents |

The rule for new writing: **teaching goes here, rationale goes in
`docs/architecture.md`, and `README.md` stays one screen.** The spec folder
is frozen — don't extend it.

## The vendored client

`app/messaging/_vendored_simple_rabbit.py` must stay **byte-identical** to
the repo-root `simple_rabbit.py`. The vendored copy is what makes the service
installable from a standalone checkout; the unit test
`tests/unit/test_vendored_client.py` fails the suite if the two drift. If you
change either file, copy it over the other **in the same commit**:

```bash
cp ../simple_rabbit.py app/messaging/_vendored_simple_rabbit.py
```

(or the reverse, if you edited the vendored copy — but prefer editing the
root file, which is the original).

## PR checklist

Paste into your PR description alongside any module definition-of-done:

```markdown
- [ ] Docs updated per the trigger table in onboarding/08-maintenance.md
      (or: no row applies)
- [ ] `.venv/bin/mkdocs build` runs clean — no broken-link warnings
- [ ] Every checked definition-of-done box was actually verified, not assumed
```

That's the whole contract. It costs a few minutes per PR and it is the only
reason page one of this guide can say "trust what you read here."
