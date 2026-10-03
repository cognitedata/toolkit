---
name: create-jira-ticket
description: Create a Jira ticket in the CDF project at cognitedata.atlassian.net. Defaults to Task or Bug issue type, component velocity:tooling, and parent epic CDF-28398 (Toolkit: Maintenance H2 2026). Use when the user wants to create a Jira ticket, log a bug, file a task, or track work in Jira.
---

# Create Jira Ticket

Read `.cursor/skills/acli-atlassian/SKILL.md` first. **Do not use the `user-atlassian` MCP.**

## Defaults

| Field        | Default value                                  |
|--------------|------------------------------------------------|
| site         | `cognitedata.atlassian.net`                    |
| projectKey   | `CDF`                                          |
| issueType    | `Task` (or `Bug` if it's a bug)                |
| component    | `velocity:tooling`                             |
| parent epic  | `CDF-28398` (Toolkit: Maintenance H2 2026)     |

Override any default when the user specifies otherwise.

## Steps

1. **Gather inputs** — ask for anything not already provided:
   - Summary (required)
   - Issue type: `Task` or `Bug` (infer from context if obvious)
   - Description (optional but recommended)
   - Any overrides to the defaults above
1. **Confirm** — show a brief summary of what will be created and ask the user to confirm before proceeding.
1. **Create the ticket** with `acli` via the Shell tool.

   Simple create (preferred when defaults apply):

   ```bash
   acli jira workitem create \
     --summary "<user-provided>" \
     --project CDF \
     --type Task \
     --description "<user-provided, if any>" \
     --parent CDF-28398 \
     --json
   ```

   If you also need component or epic-link fields that `--parent` does not set, use `--from-json`:

   ```bash
   acli jira workitem create --generate-json > /tmp/jira-create.json
   ```

   Edit the JSON (summary, type, projectKey, description, and `additionalAttributes` as needed), then:

   ```bash
   acli jira workitem create --from-json /tmp/jira-create.json --json
   ```

   Epic link field: `customfield_10014` = `"CDF-28398"`.
   Component: set via `additionalAttributes` if required by the project schema.

1. **Handle auth failures** — if the command returns `unauthorized`, stop and tell the user to run `acli auth login`. Do not fall back to the Atlassian MCP.

1. **Report back** — share the new ticket key and URL so the user can open it directly.
   URL format: `https://cognitedata.atlassian.net/browse/<KEY>`
