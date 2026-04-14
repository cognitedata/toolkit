---
name: start-it
description: >-
  Fetch Jira via Atlassian MCP (server user-atlassian), branch, plan, code.
  Use when the user says "start it" or "start" with a Jira URL.
---

# Start It

When the user says **"start it"** (or **"start"** with a Jira URL):

## 1. Get the Jira ticket

- Accept either a **Jira ticket ID** (e.g. `CDF-1234`) or a **browse URL**
  (e.g. `https://cognitedata.atlassian.net/browse/CDF-1234`).
- From a URL, use the issue key after `/browse/`.

## 2. Fetch task details

- Use the **Atlassian MCP** server. In Cursor the server id is typically **`user-atlassian`**
  (not `atlassian`). Read the MCP tool schema if invocation fails.
- **Flow:**
  1. Call **`getAccessibleAtlassianResources`** on `user-atlassian` and pick the site’s **`id`**
     as `cloudId` (e.g. Cognite: `cognitedata.atlassian.net`).
  2. Call **`getJiraIssue`** with `cloudId` and `issueIdOrKey` set to the ticket key (e.g. `CDF-1234`).
- Use the response for summary, description, status, parent epic, components, subtasks, and comments.
- Present a brief summary so the user can confirm it is the right ticket.

## 3. Create a working branch

- Run `git fetch origin main && git switch main && git pull origin main` to start from the latest main.
- Create and switch to a new branch: `git switch -c <branch-name>`.
  - Derive the branch name from the ticket: lowercase ticket ID + short slug from the summary.
  - Example: `CDF-1234` with summary "Add data product validation" → `cdf-1234/add-data-product-validation`.
- Confirm the branch name with the user before creating it.

## 4. Check if the ticket involves a new resource type

- If the Jira ticket is about adding or supporting a **new CDF resource type**, ask the user to provide either:
  - A **service contract YAML** file, or
  - A link to the **API documentation page**
- Do not proceed to implementation until this information is available, as it is needed to correctly model the resource.

## 5. Plan and start working

- Based on the Jira ticket details, create a **todo list** breaking the task into concrete implementation steps.
- Explore the codebase to understand relevant files and patterns before making changes.
- Begin working on the first todo item immediately.
