---
name: start-it
description: Fetch Jira ticket details via Atlassian MCP, create a working branch, plan implementation, and begin coding. Use when the user says "start it".
---

# Start It

When the user says **"start it"**:

## 1. Get the Jira ticket

- Ask the user for the **Jira ticket ID** (e.g. `CDF-1234`).

## 2. Fetch task details

- Use the **Atlassian MCP tool** to retrieve the ticket details (summary, description, acceptance criteria, subtasks, etc.).
- Present a brief summary of the task to the user so they can confirm it's the right ticket.

## 3. Create a working branch

- Run `git fetch origin main && git switch main && git pull origin main` to start from the latest main.
- Create and switch to a new branch: `git switch -c <branch-name>`.
  - Derive the branch name from the ticket: lowercase ticket ID + short slug from the summary.
  - Example: `CDF-1234` with summary "Add data product validation" → `cdf-1234/add-data-product-validation`.
- Confirm the branch name with the user before creating it.

## 4. Plan and start working

- Based on the Jira ticket details, create a **todo list** breaking the task into concrete implementation steps.
- Explore the codebase to understand relevant files and patterns before making changes.
- Begin working on the first todo item immediately.
