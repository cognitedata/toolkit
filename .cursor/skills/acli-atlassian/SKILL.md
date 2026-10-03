---
name: acli-atlassian
description: Read and search Cognite Jira and Confluence via the official Atlassian CLI (acli). Use instead of the user-atlassian MCP whenever the user mentions Confluence, Jira, wiki pages, tickets, or Atlassian URLs.
---

# Atlassian via acli

Do not use the `user-atlassian` MCP. It hangs. Use `acli` through the Shell tool.

Site: `cognitedata.atlassian.net`

## Auth

If a command returns `unauthorized`, stop and tell the user to run:

```bash
acli auth login
```

That OAuth login covers both Jira and Confluence. Do not prompt for an API token unless they ask. Recheck with `acli confluence auth status` and `acli jira auth status`.

## Confluence

Page ID is the number after `/pages/` in the URL.

```bash
acli confluence page view --id 6141215047 --body-format storage
acli confluence page view --id 6141215047 --json
acli confluence space list
acli confluence space view --key PD
```

`--body-format storage` for HTML-ish source. `--body-format view` for readable text. `--json` when parsing.

Confluence **page write is not in acli** (only `page view`). For create/update, say so and wait; do not fall back to the Atlassian MCP.

## Jira

```bash
acli jira workitem view CDF-12345
acli jira workitem view CDF-12345 --json
acli jira workitem view CDF-12345 --fields summary,description,status,issuetype,components,comment
acli jira workitem search --jql "project = CDF AND text ~ \"toolkit\"" --limit 20
acli jira workitem create --summary "..." --project CDF --type Task
```

Default site after login is Cognite. Pass `--json` when the output will be parsed.

## Failures

- `command not found`: install with `brew tap atlassian/homebrew-acli && brew install acli` (trust the tap if Homebrew asks).
- Timeout or MCP tools appearing: ignore them; retry with `acli`.
