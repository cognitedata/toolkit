---
name: send-it
description: Stage changes, run pre-commit hooks, and optionally commit, push, create a PR, and monitor CI. Use when the user says "send it".
---

# Send It / Stage It

When the user says **"send it"**:

## 1. Stage and pre-commit

- Run `git status` to see all changed and untracked files.
- Only stage files that were **created or modified as part of this task**. Use `git add <file>...` with explicit paths.
- If there are unstaged changes in files **not related to this task**, list them and ask the user
  what to do (stage them too, ignore, stash, etc.). Do not silently include unrelated changes.
- Run `uv run pre-commit run --all-files` to validate staged changes.
- If pre-commit hooks fail, fix every reported issue, re-stage the fixes, and re-run. Repeat until all hooks pass.

## 2. Ask: stage only or commit?

Use the **AskQuestion** tool to ask the user:

> **"All pre-commit hooks pass. What would you like to do?"**
>
> - **Stage only** — leave changes staged for local review
> - **Commit and push** — commit, push, and open/update a PR

If the user chose **Stage only**, stop here.

## 3. Commit

- Run `git diff --cached` and `git log --oneline -5` to review staged changes and match the repo's commit style.
- Write a concise commit message summarizing the changes.
- Run `git commit` with that message.

## 4. Sync and push

- Run `git pull origin main` to merge latest main into the branch.
- Run `git pull origin` to sync the current branch (ignore errors if the remote branch doesn't exist yet).
- Run `git push -u origin HEAD` to push (sets upstream if needed).

## 5. PR creation (if needed)

- Check if a PR exists: `gh pr view --json url 2>/dev/null`.
- **If no PR exists**, suggest creating one:
  - Ask the user for the Jira ticket ID.
  - Propose a title: `[TICKET-ID] Description of changes`.
  - Draft a body by reading `.github/pull_request_template.md` and filling it in
    using `git log main..HEAD --oneline`.
  - Show the proposed PR title and body, and **ask for confirmation** before creating it.
  - If the user confirms, create the PR as a **draft**:

    ```bash
    gh pr create --title "..." --body "..." --base main --draft
    ```

  - After creation, verify the PR body doesn't contain "Made with Cursor". If it does,
    suggest the user disable this in Cursor settings:
    **Settings > General > uncheck "Include 'Made with Cursor' in PRs"**.
  - Comment "/gemini review" on the PR to request a review from Gemini.

## 6. Monitor CI

- Launch a **background agent** (`subagent_type="shell"`) to run `gh pr checks --watch`.
- When checks complete, report back: **green** (all passing) or **red** (with names of failed checks).
- **If all checks pass**, mark the PR as ready for review: `gh pr ready`.
- **If checks fail:**
  - For each failed check, fetch its logs with `gh pr checks` and
    `gh run view <run-id> --log-failed`.
  - **Transient failures** (network timeouts, flaky tests, rate limits, runner issues):
    re-run the failed job once with `gh run rerun <run-id> --failed`.
  - **Non-transient failures** (lint errors, test assertions, type errors, build failures):
    read the logs, fix the underlying code issue, then run the full "send it" flow again
    (stage, commit, push, monitor).
