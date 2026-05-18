# TDL-0004: Error vs Warning Semantics and Phase Boundaries

**Date:** 2026-05-15

## What

The toolkit has **three** parallel severity systems coexisting today,
with inconsistent user-facing semantics:

1. **`ToolkitError`** (`cognite_toolkit/_cdf_tk/exceptions.py`) — a Python
   exception. Raised at the call site, caught at the CLI root, aborts the
   command immediately. Roughly 30 subclasses (`ToolkitValidationError`,
   `ToolkitMissingResourceError`, etc.).
2. **`SeverityLevel` + `ToolkitWarning`**
   (`cognite_toolkit/_cdf_tk/tk_warnings/base.py`) — the legacy warning
   system. String enum `ERROR / HIGH / MEDIUM / LOW`. 41 warning classes
   use it. `SeverityLevel.ERROR` renders as `ERROR [ERROR]:` but the
   command **continues running** — used in one place today
   (`fileread.py:220`).
3. **`InsightDefinition` + numeric severity**
   (`cognite_toolkit/_cdf_tk/commands/build_v2/data_classes/_insights.py`)
   — the build v2 system. Integer severities: `FileReadError=60`,
   `FailedValidation=60`, `ModelSyntaxWarning=40`, `ConsistencyError=30`,
   `IgnoredFileWarning=20`, `Recommendation=10`. Build v2 buckets them
   with thresholds (`≤15`, `≤35`, `>35`). `InsightList.has_errors`
   already exists and is wired to fail the phase — but only for build v2.

A fourth, narrower `Severity` enum (`skipped / warning / failure`) exists
in `dataio/logger.py` for data IO aggregation.

A user seeing `ERROR: ...` in build output has no way to predict whether
the command will fail. Worse: in build v2 the class names and the numeric
severities disagree — `ModelSyntaxWarning` (severity 40) outranks
`ConsistencyError` (severity 30), so a class literally called "Error" is
*less severe* than one called "Warning". This is the encoded form of the
bug Magnus reported in
[CDF-27907](https://cognitedata.atlassian.net/browse/CDF-27907) and
discussed in #contributors-toolkit on 2026-05-08.

We also have no shared definition of a **phase boundary**, which is the
natural place to aggregate findings and decide pass/fail.

## Goal

**A command runs to completion, collecting warnings and errors, and
reports them at the next phase boundary — unless a `ToolkitError` is
raised.**

`ToolkitError` is reserved for conditions that are by definition
unrecoverable: there is no way to proceed, and continuing would produce
garbage or hide more useful findings. The user must address the
underlying cause before it makes sense to retry. We accept that these
abort immediately at the raise site.

Everything else — schema mismatches, missing references, unresolved
variables, naming-convention violations, recommendations — is collected
as the command runs. At the phase boundary the toolkit prints the full
set in one pass and decides pass/fail based on whether any of the
collected issues are errors. The user sees every problem of a given kind
at once instead of fixing them one at a time, rerunning, and hitting the
next.

## Decision

We adopt the **collect-then-fail-at-phase-boundary** model (the same
model used by `ruff`, `mypy`, and `tsc`). Three categories, three
contracts:

### 1. Warnings — informational, never block

Severities `WARNING`, `NOTICE`, `HINT` (exact names TBD with Magnus).
Collected and rendered. The command **always proceeds** and exits 0 from
these alone. Users can suppress specific warning classes via `cdf.toml`
(planned).

### 2. Errors — collected, reported in bulk, fail the phase

`SeverityLevel.ERROR` issues are collected during a phase. At the
**phase boundary**, the toolkit:

1. Prints every error collected during the phase, grouped and with
   source paths.
2. Prints a summary line (`Found N errors in M files`).
3. Exits non-zero. No further phases run.

The user sees **all** failure modes from that phase in a single pass
instead of fix-one-rerun whack-a-mole. The `WarningList` (or its
successor) gains a `has_errors()` method that command entry points
consult to set the exit code.

### 3. `ToolkitError` exceptions — abort immediately

Reserved for conditions that genuinely cannot be collected and continued
past: missing auth, network/IO failure, unreachable CDF project,
internal invariant violations. These still raise, are caught at the CLI
root, and abort at the raise site.

The intuition: if continuing past the condition would produce garbage or
hide more useful findings, raise `ToolkitError`. If continuing would
surface *more* useful findings of the same kind, emit a
`SeverityLevel.ERROR` and let the phase boundary handle the exit code.

### Rendered prefixes (user-facing contract)

- `ERROR:` — this will fail the build/phase. Every `ERROR:` you see is
  shown before exit; fix them all before rerunning.
- `WARNING:` / `NOTICE:` / `HINT:` — fyi, the command continues.

## Phase boundary — definition

A **phase boundary** is the point in a command's execution where the
toolkit finishes a logically complete unit of work and can meaningfully
decide "proceed or stop." Concretely in the toolkit today:

| Command | Phases (in order) | Boundary behaviour |
| --- | --- | --- |
| `cdf build` | 1. Read & parse modules → 2. Validate → 3. Write build dir | Collect errors across phases 1 and 2. Fail at the end of phase 2 before any files are written. |
| `cdf deploy` | 1. Load build dir → 2. Plan/diff → 3. Apply per resource type | Collect errors during phases 1 and 2. Fail at end of phase 2. Phase 3 stays fail-fast (side effects on CDF). |
| `cdf clean` | 1. Load build dir → 2. Plan deletions → 3. Apply | Same shape as `deploy`. |
| `cdf modules <add\|init>` | Single phase | No aggregation needed; raise `ToolkitError` on failure. |

The principle: **aggregate within a phase, fail at its boundary, do not
enter the next phase if the previous one had errors.** Phases with side
effects on CDF (the actual API calls in `deploy`/`clean`/`purge`) remain
fail-fast — once you've started mutating CDF, accumulating more errors
helps no one.

A phase boundary is *not* the same as a Python function return or a CLI
command exit. It is the architectural seam between "gathering
information" and "acting on it." `build` has one such seam (validate →
write). `deploy` has one (plan → apply). That is the only place an
aggregated error report is appropriate.

## Grounding in `ruff`

`ruff` is the closest precedent for the model we're adopting, and most
of our intended users already use it daily. Concretely:

### What we take from `ruff` directly

- **Run to completion, fail at the end.** `ruff check` visits every
  file, prints every violation, then exits non-zero iff any violations
  were found. Same shape as our build-phase boundary.
- **Stable rule codes.** Every `ruff` rule has a short identifier
  (`E501`, `F401`, `B008`). The code is stable across versions and
  releases even when class internals change, and it's what users put in
  `# noqa` and `[tool.ruff.lint] ignore`. We should adopt the same:
  every toolkit issue type gets a code (e.g. `TK001` for
  `UnresolvedVariable`, `TK010` for `NamingConvention`) and the
  `cdf.toml` suppression list keys on codes, not class names. Renaming
  a class is then non-breaking for users.
- **Per-rule documentation.** `ruff` has one doc page per rule with a
  bad/good example and a "why" paragraph. We should do the same — each
  rule code links to a docs entry that explains the failure and how to
  fix it. Drives down "what does this warning even mean?" support
  traffic.
- **Selection and suppression are user concerns, not author concerns.**
  In `ruff`, the rule author doesn't decide whether a rule should be
  shown — the user does, via `select` / `ignore` in config. This is the
  shape we want for the planned `cdf.toml` suppression: the toolkit
  emits everything that applies, the user opts out of what they don't
  want.

### Where we adapt rather than copy

- **`ruff` has no severity dial.** Every violation is just a violation;
  the user decides which rules to enable. We can't go that far because
  some of our findings (e.g. `Recommendation`) are genuinely advisory
  and not worth failing a build over. So we keep a small severity
  enum, but we make it categorical (the three buckets above) instead of
  a numeric dial. Closest `ruff` analogue: the distinction between
  "rule violation" (fails the run) and "preview rule" (informational).
- **`ruff` is read-only.** We have `ToolkitError` exceptions for
  unrecoverable conditions because we make real CDF calls and write to
  disk. `ruff` doesn't need that category; we do.
- **No `--fix` (yet).** `ruff --fix` is one of its best features but
  out of scope here. A future TDL can extend the contract to cover
  auto-fixable issues — the rule-code model leaves room for it.

### Mapping to the toolkit

| `ruff` concept | Toolkit equivalent |
| --- | --- |
| Rule violation | `SeverityLevel.ERROR` issue, collected, fails the phase |
| Preview rule | `WARNING` / `NOTICE` / `HINT`, collected, never fails |
| Rule code (`E501`) | Toolkit rule code (`TK001`, etc.) |
| `# noqa: E501` | Out of scope — files are user data, not source we own |
| `[tool.ruff.lint] ignore = [...]` | `[tool.cdf.warnings] ignore = [...]` |
| `ruff` exits non-zero on any violation | Toolkit exits non-zero iff `has_errors()` |
| Crash before completion | `ToolkitError` raised |

The user-facing mental model becomes: "this works like `ruff`, but for
your CDF modules." That's a one-sentence explanation that lands with
the engineers we ship to.

## Why

- **Eliminates whack-a-mole.** Users with messy YAML get one
  comprehensive report instead of fixing one error, rerunning, hitting
  the next, rerunning, repeat. This is the single biggest CLI UX win.
- **Removes the `ERROR:`-but-build-passes ambiguity** flagged in
  CDF-27907 — `ERROR:` always means "the phase failed."
- **Keeps `ToolkitError` exceptions for what they are good at** —
  unrecoverable conditions where collecting more state is pointless or
  unsafe.
- **Matches the model users already know** from `ruff` (see above for
  details), `mypy`, `tsc`, `eslint`. Familiar mental model, no
  toolkit-specific re-learning.
- **Side-effect commands stay safe.** Because `deploy`'s mutation phase
  stays fail-fast, we don't introduce the failure mode "kept calling
  CDF after the first 400 to collect more 400s."

## Consequences

- `WarningList` needs a `has_errors()` method and command entry points
  need to consult it when setting the exit code. The build v2
  `InsightList` already has this — it should be the reference
  implementation, and the legacy `WarningList` should gain the same
  wiring (or the two should converge).
- Every place currently using `raise ToolkitValidationError(...)`
  mid-validation needs review: if the surrounding loop can continue and
  surface more findings, it should emit an error-level issue instead of
  raising. If continuing would corrupt state, keep the raise.
- The legacy `SeverityLevel` enum will be reshaped (see follow-up plan
  and CDF-27907) so the rendered prefix matches this contract.
- Build v2 class names need to align with their numeric severity:
  `ModelSyntaxWarning` (40) → rename to indicate it's an error;
  `ConsistencyError` (30) — either rename to a warning level or
  increase the number above syntax. Tracked as part of
  [CDF-27968](https://cognitedata.atlassian.net/browse/CDF-27968).
- Long-term: converge the three systems into one. The legacy
  `tk_warnings`, the build v2 `_insights`, and the `dataio/logger`
  `Severity` all model the same concept differently. Picking one model
  and migrating is its own work item, larger than this TDL.
- Assign a stable rule code (`TK001`, `TK002`, …) to every issue type
  before we ship user-visible suppression. Without codes, the
  suppression list keys on class names and breaks every time we
  rename. This is the lesson `ruff` learned the hard way; we get it
  for free if we adopt codes from day one.
