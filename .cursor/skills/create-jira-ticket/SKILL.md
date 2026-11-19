---
name: create-jira-ticket
description: Create a Jira ticket in the CDF project at cognitedata.atlassian.net. Defaults to Task or Bug issue type, component Velocity:Tooling (id 11354), and parent epic CDF-28398 (Toolkit: Maintenance H2 2026). Use when the user wants to create a Jira ticket, log a bug, file a task, or track work in Jira.
---

# Create Jira Ticket

## Defaults

| Field        | Default value                                  |
|--------------|------------------------------------------------|
| cloudId      | `cognitedata.atlassian.net`                    |
| projectKey   | `CDF`                                          |
| issueType    | `Task` (or `Bug` if it's a bug)                |
| component    | id `11354` (`Velocity:Tooling`)                |
| parent epic  | `CDF-28398` (Toolkit: Maintenance H2 2026)     |

Override any default when the user specifies otherwise.

## Steps

1. **Gather inputs** — ask for anything not already provided:
   - Summary (required)
   - Issue type: `Task` or `Bug` (infer from context if obvious)
   - Description (optional but recommended)
   - Any overrides to the defaults above
1. **Confirm** — show a brief summary of what will be created and ask the user to confirm before proceeding.
1. **Create the ticket** with `acli` (not the Atlassian MCP). Write the payload to a temporary JSON file;
   the description uses Atlassian Document Format:

   ```json
   {
     "additionalAttributes": {"components": [{"id": "11354"}]},
     "description": {"type": "doc", "version": 1, "content": [
       {"type": "paragraph", "content": [{"type": "text", "text": "<description>"}]}
     ]},
     "parentIssueId": "CDF-28398",
     "projectKey": "CDF",
     "summary": "<summary>",
     "type": "Task"
   }
   ```

   ```bash
   acli jira workitem create --from-json <file> --json
   ```

   The `--json` output is large; save it to a file. The new key is its top-level `key` field. Alternatively,
   search for it (the search index can lag a few seconds behind):

   ```bash
   acli jira workitem search --jql 'project = CDF AND reporter = currentUser() AND created >= -10m' --fields key,summary
   ```

   An `assignee` in the JSON is overridden by the component's default assignee, so assign afterwards:

   ```bash
   acli jira workitem assign --key <KEY> --assignee @me --yes
   ```

   If `acli` reports unauthorized, ask the user to run `acli auth login`.
1. **Report back** — share the new ticket key and URL so the user can open it directly.
   URL format: `https://cognitedata.atlassian.net/browse/<KEY>`
