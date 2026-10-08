# TDL-0005: Insight Code Naming

**Date:** 2026-10-08

## What

Insights (errors, warnings, recommendations) are identified by a `code`. Codes were previously inconsistent
(e.g. `MISSING-SUFFIX`, `READ-ERROR`, `UNKNOWN-REFERENCE`, `AUTH-001`), so users could not tell from a code whether a
problem was certain or only suspected, and could not predict how to write the code in an ignore rule
(`# rules: ignore[CODE]`). Insights also carried a separate hand-written `title` that duplicated the code.

## Decision

1. Codes follow `<PROBLEM>-<SUBJECT>`, with the problem word first, e.g. `MISSING-FILE-SUFFIX`, `INVALID-VALUE`.
2. The problem word carries the certainty of the finding:
   - `MISSING-*`, `INVALID-*`, `EXCEEDED-*`: we know there is a problem.
   - `UNVERIFIED-*`, `UNRECOGNIZED-*`: we cannot confirm it, so we only guess. These are always warnings.
3. The heading shown to the user is derived from the code, e.g. `INVALID-AGENT-MODEL` is
   shown as "Invalid agent model". Details that distinguish cases sharing a code (e.g. container vs. view) belong in
   the message.

## Why

A consistent `<PROBLEM>-<SUBJECT>` scheme makes codes predictable and greppable, and lets users read the certainty of
a finding straight from the code. Putting the problem first makes the code read as a natural English heading, so the
title can be derived instead of maintained by hand at every emit site.
