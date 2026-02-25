---
name: send-it
description: Stage, commit, push changes, create a PR, and monitor CI. Handles pre-commit failures and CI retries. Use when the user says "send it".
---

# Send It

When the user says **"send it"**:

## 1. Stage and pre-commit

- Read and follow the [stage-it skill](../stage-it/SKILL.md) to stage changes and pass all pre-commit hooks.

## 2. Commit

- Run `git diff --cached` and `git log --oneline -5` to review staged changes and match the repo's commit style.
- Write a concise commit message summarizing the changes.
- Run `git commit` with that message.

## 3. Sync and push

- Run `git pull origin main` to merge latest main into the branch.
- Run `git pull origin` to sync the current branch (ignore errors if the remote branch doesn't exist yet).
- Run `git push -u origin HEAD` to push (sets upstream if needed).

## 4. PR creation (if needed)

- Check if a PR exists: `gh pr view --json url 2>/dev/null`.
- **If no PR exists**, suggest creating one:
  - Ask the user for the Jira ticket ID.
  - Propose a title: `[TICKET-ID] Description of changes`.
  - Draft a body by reading `.github/pull_request_template.md` and filling it in
    using `git log main..HEAD --oneline`. **Never** append "made with Cursor" to the body.
  - Show the proposed PR title and body, and **ask for confirmation** before creating it.
  - If the user confirms, create the PR using `gh api repos/{owner}/{repo}/pulls`
    to avoid the automatic "Made with Cursor" footer that `gh pr create` appends. Example:

    ```bash
    gh api repos/OWNER/REPO/pulls \
      -f title="..." -f body='...' \
      -f head="BRANCH" -f base="main" --jq '.html_url'
    ```

  - Comment "/gemini review" on the PR to request a review from Gemini.

## 5. Monitor CI

- Launch a **background agent** (`subagent_type="shell"`) to run `gh pr checks --watch`.
- When checks complete, report back: **green** (all passing) or **red** (with names of failed checks).
- **If checks fail:**
  - For each failed check, fetch its logs with `gh pr checks` and
    `gh run view <run-id> --log-failed`.
  - **Transient failures** (network timeouts, flaky tests, rate limits, runner issues):
    re-run the failed job once with `gh run rerun <run-id> --failed`.
  - **Non-transient failures** (lint errors, test assertions, type errors, build failures):
    read the logs, fix the underlying code issue, then run the full "send it" flow again
    (stage, commit, push, monitor).
