# TDL-0004: Error vs Warning Semantics and Phase Boundaries

**Date:** 2026-05-15

## What

Errors, Warnings and Insights are vital in Toolkit because they guide
users towards configuration quality, correctness and consistency.

Classification helps unify the user experience and control flow so
that the user can resolve issues faster, while reducing noise and
warning fatigue.

Today the toolkit has three coexisting severity systems with
inconsistent semantics: legacy `SeverityLevel` (`ERROR / HIGH /
MEDIUM / LOW`), build v2 numeric `InsightDefinition` (10/20/30/40/60
with threshold buckets), and a narrower `dataio/logger.Severity`. A
user seeing `ERROR: ...` cannot predict whether the command will
fail; in build v2 the class names and numbers even disagree
(`ModelSyntaxWarning=40` outranks `ConsistencyError=30`). Reported in
[CDF-27907](https://cognitedata.atlassian.net/browse/CDF-27907).

## Decision

Adopt the **collect-then-fail-at-phase-boundary** model used by
`ruff`, `mypy`, and `tsc`. A command runs to completion, collecting
issues, and reports them at the next phase boundary. The only
exception is `ToolkitError`, which aborts immediately.

### Configuration quality — three categorical severities

Findings about the user's modules:

| Severity | Author rule | Phase behaviour |
| --- | --- | --- |
| `ERROR` | The toolkit cannot honour the user's intent | Collected, fails the phase |
| `WARNING` | The toolkit thinks something specific is probably wrong | Collected, never fails |
| `HINT` | Advisory; a recommended practice or stylistic preference | Collected, never fails |

Examples: `ERROR` — `UnresolvedVariable`, `MissingRequiredParameter`.
`WARNING` — `TemplateVariable` ("CHANGE_ME"), `UnknownResourceType`
(typo). `HINT` — `DataSetMissing`, `NamingConvention`.

### Technical exceptions — `ToolkitError`

A fourth category, outside the severity system. About the environment
the toolkit runs in (auth, network, file IO, internal invariant), not
about the user's modules. Raised and caught at the CLI root; aborts
immediately. There is nothing to collect.

| | Configuration quality | `ToolkitError` |
| --- | --- | --- |
| **About** | User's modules | Environment toolkit runs in |
| **Who fixes** | User edits modules, reruns | User fixes precondition, retries |
| **Collected?** | Yes, in bulk at phase boundary | No, single message, aborts |
| **Examples** | unresolved variable, missing field | missing IDP secret, unreachable CDF |
| **Stack trace** | Never | Only under `--verbose` (see [CDF-27617](https://cognitedata.atlassian.net/browse/CDF-27617)) |

### Rendered prefixes (user-facing contract)

- `ERROR:` — fails the phase. Fix all of them before rerunning.
- `WARNING:` — probably wrong; command continues.
- `HINT:` — advisory; command continues.
- `ToolkitError` (own panel, no prefix) — toolkit couldn't proceed.
  Fix the precondition.

## Phase boundary

A **phase boundary** is the architectural seam between *gathering
information* and *acting on it*. Issues are aggregated within a phase
and the phase decides pass/fail at its boundary; phases with side
effects on CDF stay fail-fast.

| Command | Phases | Boundary behaviour |
| --- | --- | --- |
| `cdf build` | parse → validate → write | Aggregate across parse + validate. Fail before writing. |
| `cdf deploy` | load → plan → apply | Aggregate across load + plan. Fail before applying. Apply phase stays fail-fast (CDF side effects). |
| `cdf clean` | load → plan → apply | Same shape as `deploy`. |
| `cdf modules <add\|init>` | single phase | No aggregation; raise `ToolkitError` on failure. |

## Why

- **Eliminates whack-a-mole.** Users see all findings of a given kind
  in one pass instead of fix-one-rerun-fix-next.
- **Removes the `ERROR:`-but-build-passes ambiguity** flagged in
  CDF-27907.
- **Categorical severity removes the LOW-vs-MEDIUM bikeshed.** Author
  rule is mechanical: "can the toolkit honour the user's intent?"
- **Matches `ruff`'s model** that most of our users already know.
  Suppression by rule code (planned, separate ticket) follows the
  same pattern.

## Consequences

- Add `has_errors()` on the collected-issues container and consult it
  for exit codes. Build v2's `InsightList.has_errors` is the reference.
- Reclassify the current `HIGH`-severity warnings whose conditions
  mean "toolkit cannot honour intent" as `ERROR`s. Tracked in
  [CDF-27968](https://cognitedata.atlassian.net/browse/CDF-27968).
- Rename build v2 classes so name ↔ severity agree
  (`ModelSyntaxWarning` → `ModelSyntaxError`). Coordinate with NEAT
  owner since the adapter at `rules/_neat.py:101` is where the rename
  originated.
- Long-term: converge the three severity systems
  (`tk_warnings.SeverityLevel`, build v2 `InsightDefinition`,
  `dataio/logger.Severity`) into one. Separate work item.
- Assign stable rule codes (`TK001`, `TK002`, …) to every issue type
  before shipping user-visible suppression, so renaming a class
  doesn't break user `cdf.toml` config.
