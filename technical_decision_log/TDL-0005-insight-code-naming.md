# TDL-0005: Insight Code and Title Naming

**Date:** 2026-10-08

## What

Insights (errors, warnings, recommendations) are identified by a `code` and shown to the user with a `title`.
Codes were previously inconsistent (e.g. `MISSING-SUFFIX`, `READ-ERROR`, `UNKNOWN-REFERENCE`, `AUTH-001`), so users
could not tell from a code whether a problem was certain or only suspected, and could not predict how to write the
code in an ignore rule (`# rules: ignore[CODE]`).

## Decision

1. Codes follow the format `<SUBJECT>-<PROBLEM>`, with the problem word last, e.g. `FILE-SUFFIX-MISSING`, `VALUE-INVALID`.
2. The problem word carries the certainty of the finding:
   - `*-MISSING`, `*-INVALID`, `*-EXCEEDED`: we know there is a problem.
   - `*-UNVERIFIED`, `*-UNRECOGNIZED`: we cannot confirm it, so we only guess. These are always warnings.
3. The `title` is the human-readable heading shown for the code, e.g. `Invalid agent model`. It is a plain string at the
   emit site, not part of the code.

## Why

A consistent `<SUBJECT>-<PROBLEM>` scheme makes codes predictable and greppable, and lets users read the severity of
certainty straight from the code. Keeping the problem word last allows grouping by subject and keeps ignore rules easy
to write.
