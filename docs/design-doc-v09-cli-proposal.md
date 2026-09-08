# Design Doc: v09 CLI command restructure

**Status:** Proposal  
**Feature gate:** `alpha_flags.v09` in `cdf.toml`  
**Jira:** [CDF-28574](https://cognitedata.atlassian.net/browse/CDF-28574)  
**Parent design:** [Cognite CLI design](https://cognitedata.atlassian.net/wiki/spaces/DU/pages/6100517022/Cognite+CLI+design)

---

## Summary

Introduce a v0.9 CLI command layout behind an `alpha_flags.v09` gate. Session commands will live under
`cdf auth` (gh-style; `login`/`logout` in a follow-up PR). Bootstrap folds under `cdf init` with separate
`auth` and `access` topics. Split `cdf auth verify` into read (`auth status`) and write (`init access` +
`api functions activate`). `cdf build` and `cdf deploy` unchanged.

---

## Sequencing

1. **This PR (CDF-28574)** — v09 layout from `main` (no dependency on #3174)
2. **Follow-up** — `cdf auth login` / `logout` + session-aware `auth status` (#3174)
3. **Follow-up** — session wired into build/deploy; remove deprecated commands in 0.9 GA

See [CDF-28574](https://cognitedata.atlassian.net/browse/CDF-28574) for the full command tree, mapping
table, and implementation scope.
