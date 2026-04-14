---
name: create-jira-ticket
description: Create a Jira ticket in the CDF project at cognitedata.atlassian.net. Defaults to Task or Bug issue type, component velocity:tooling, and parent epic CDF-26605 (Toolkit: Maintenance H1 2026). Use when the user wants to create a Jira ticket, log a bug, file a task, or track work in Jira.
---

# Create Jira Ticket

## Defaults

| Field        | Default value                                  |
|--------------|------------------------------------------------|
| cloudId      | `cognitedata.atlassian.net`                    |
| projectKey   | `CDF`                                          |
| issueType    | `Task` (or `Bug` if it's a bug)                |
| component    | `velocity:tooling`                             |
| parent epic  | `CDF-26605` (Toolkit: Maintenance H1 2026)     |

Override any default when the user specifies otherwise.

## Steps

1. **Gather inputs** — ask for anything not already provided:
   - Summary (required)
   - Issue type: `Task` or `Bug` (infer from context if obvious)
   - Description (optional but recommended)
   - Any overrides to the defaults above
1. **Confirm** — show a brief summary of what will be created and ask the user to confirm before proceeding.
1. **Create the ticket** using the Atlassian MCP `createJiraIssue` tool:

   ```yaml
   cloudId:       cognitedata.atlassian.net
   projectKey:    CDF
   issueTypeName: Task   (or Bug)
   summary:       <user-provided>
   description:   <user-provided, if any>
   parent:        CDF-26605
   contentFormat: markdown
   additional_fields:
     components:
       - name: velocity:tooling
   ```

1. **Report back** — share the new ticket key and URL so the user can open it directly.
   URL format: `https://cognitedata.atlassian.net/browse/<KEY>`
