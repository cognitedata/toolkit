# TDL-0004: Error vs Warning Semantics and Phase Boundaries

**Date:** 2026-05-15

## What

Errors, Warnings and Insights are vital communication elements in Toolkit because they guide users towards
higher configuration quality and integrity.

Clearer categories give users better ability to distinguish between issues and warnings, and understand the
experience and control flow so they can resolve issues faster, while reducing noise and warning fatigue.

Today the toolkit has three coexisting severity systems with
inconsistent semantics: legacy `SeverityLevel` (`ERROR / HIGH /
MEDIUM / LOW`), build v2 numeric `InsightDefinition` (10/20/30/40/60
with threshold buckets), and a narrower `dataio/logger.Severity`. A
user seeing `ERROR: ...` cannot predict whether the command will
fail; in build v2 the class names and numbers even disagree
(`ModelSyntaxWarning=40` outranks `ConsistencyError=30`). Reported in
[CDF-27907](https://cognitedata.atlassian.net/browse/CDF-27907).

## Decision

1. Adopt the **collect-then-fail-at-phase-boundary** model used by
`ruff`, `mypy`, and `tsc`. A command runs to completion, collecting
issues, and reports them at the next phase boundary. The only
exception is `ToolkitError`, which aborts immediately.
2. Consolidate and adopt the three categorical severities: `ERROR`, `WARNING`, and `HINT`. Allow for
   `ToolkitError` as exceptions raised on the call site.
3. Separate presentation from severity: let the UI decide how to order, sort, and render the issues,
   while the severity dictates the phase boundary behavior.

### Details

Configuration integrity — three categorical severities (findings about the user's modules):

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

| | Configuration integrity | `ToolkitError` |
| --- | --- | --- |
| **About** | User's modules | Environment toolkit runs in |
| **Who fixes** | User edits modules, reruns | User fixes precondition, retries |
| **Collected?** | Yes, in bulk at phase boundary | No, single message, aborts |
| **Examples** | unresolved variable, missing field | missing IDP secret, unreachable CDF |
| **Stack trace** | Never | Only under `--verbose` (see [CDF-27617](https://cognitedata.atlassian.net/browse/CDF-27617)) |

### Phase boundary

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
