"""
Script to create Jira tasks from uncompleted items in tasks.md.

Usage:
    # Dry run (preview tasks that would be created):
    python create_jira_tasks.py --dry-run

    # Create tasks in Jira:
    python create_jira_tasks.py

Environment Variables Required:
    JIRA_URL        - Your Jira instance URL (e.g., https://company.atlassian.net)
    JIRA_EMAIL      - Your Jira account email
    JIRA_API_TOKEN  - Your Jira API token (generate at https://id.atlassian.com/manage-profile/security/api-tokens)
    JIRA_PROJECT    - Jira project key (e.g., TK)
    JIRA_EPIC_KEY   - Epic key to link tasks to (e.g., TK-123)

Optional Environment Variables:
    JIRA_COMPONENT  - Component name to assign to tasks (e.g., Backend)

Dependencies:
    pip install requests
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv

load_dotenv(".env")  # Load environment variables from .env file if present

@dataclass
class Task:
    """Represents a task extracted from the markdown file."""

    section: str
    resource_name: str
    description: str

    @property
    def summary(self) -> str:
        """Generate a Jira-friendly summary."""
        return f"[{self.resource_name}] {self.description}"

    @property
    def full_description(self) -> str:
        """Generate a detailed description for the Jira issue (Atlassian Document Format)."""
        # Using Atlassian Document Format (ADF) for Jira Cloud API v3
        return {
            "type": "doc",
            "version": 1,
            "content": [
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "Section: ", "marks": [{"type": "strong"}]},
                        {"type": "text", "text": self.section},
                    ],
                },
                {
                    "type": "paragraph",
                    "content": [
                        {"type": "text", "text": "Resource: ", "marks": [{"type": "strong"}]},
                        {"type": "text", "text": self.resource_name},
                    ],
                },
                {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": self.description}],
                },
                {"type": "rule"},
                {
                    "type": "paragraph",
                    "content": [
                        {
                            "type": "text",
                            "text": "Auto-generated from tasks.md",
                            "marks": [{"type": "em"}],
                        }
                    ],
                },
            ],
        }


def parse_tasks_file(file_path: Path) -> list[Task]:
    """Parse the tasks.md file and extract uncompleted tasks.

    Args:
        file_path: Path to the tasks.md file.

    Returns:
        List of Task objects for uncompleted items.
    """
    content = file_path.read_text(encoding="utf-8")
    lines = content.split("\n")

    tasks: list[Task] = []
    current_section = ""
    current_resource = ""

    # Pattern for section headers (## Section Name)
    section_pattern = re.compile(r"^##\s+(.+)$")
    # Pattern for resource headers (### ResourceName)
    resource_pattern = re.compile(r"^###\s+(\w+).*$")
    # Pattern for uncompleted tasks (- [ ] Task description)
    uncompleted_pattern = re.compile(r"^-\s+\[\s\]\s+(.+)$")

    for line in lines:
        # Check for section header
        section_match = section_pattern.match(line)
        if section_match:
            current_section = section_match.group(1).strip()
            continue

        # Check for resource header
        resource_match = resource_pattern.match(line)
        if resource_match:
            current_resource = resource_match.group(1).strip()
            continue

        # Check for uncompleted task
        task_match = uncompleted_pattern.match(line)
        if task_match:
            description = task_match.group(1).strip()
            # Clean up backticks and other markdown formatting
            description = description.replace("`", "")

            task = Task(
                section=current_section,
                resource_name=current_resource,
                description=description,
            )
            tasks.append(task)

    return tasks


def get_jira_config() -> dict[str, str | None]:
    """Get Jira configuration from environment variables.

    Returns:
        Dictionary with Jira configuration.

    Raises:
        SystemExit: If required environment variables are missing.
    """
    required_vars = ["JIRA_URL", "JIRA_EMAIL", "JIRA_API_TOKEN", "JIRA_PROJECT", "JIRA_EPIC_KEY"]
    optional_vars = ["JIRA_COMPONENT"]
    config: dict[str, str | None] = {}
    missing: list[str] = []

    for var in required_vars:
        value = os.environ.get(var)
        if not value:
            missing.append(var)
        else:
            config[var.lower()] = value

    # Add optional variables (can be None)
    for var in optional_vars:
        config[var.lower()] = os.environ.get(var)

    if missing:
        print("Error: Missing required environment variables:")
        for var in missing:
            print(f"  - {var}")
        print("\nPlease set these variables before running the script.")
        sys.exit(1)

    return config


class JiraClient:
    """Simple Jira REST API client."""

    def __init__(self, url: str, email: str, api_token: str) -> None:
        self.base_url = url.rstrip("/")
        self.auth = HTTPBasicAuth(email, api_token)
        self.headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def get_issue(self, issue_key: str) -> dict | None:
        """Fetch an issue by key."""
        url = f"{self.base_url}/rest/api/3/issue/{issue_key}"
        response = requests.get(url, headers=self.headers, auth=self.auth, timeout=30)

        if response.status_code == 200:
            return response.json()
        return None

    def search_epics(self, project_key: str, max_results: int = 20) -> list[dict]:
        """Search for epic issues in a project.

        Args:
            project_key: The project key to search in.
            max_results: Maximum number of epics to return.

        Returns:
            List of epic issues with key and summary.
        """
        url = f"{self.base_url}/rest/api/3/search/jql"
        jql = f'project = "{project_key}" AND issuetype = Epic ORDER BY created DESC'
        payload = {
            "jql": jql,
            "maxResults": max_results,
            "fields": ["summary", "status"],
        }

        response = requests.post(url, json=payload, headers=self.headers, auth=self.auth, timeout=30)

        if response.status_code == 200:
            data = response.json()
            return [
                {
                    "key": issue["key"],
                    "summary": issue["fields"]["summary"],
                    "status": issue["fields"]["status"]["name"],
                }
                for issue in data.get("issues", [])
            ]
        return []

    def create_issue(
        self,
        project_key: str,
        summary: str,
        description: dict,
        issue_type: str = "Task",
        parent_key: str | None = None,
        component: str | None = None,
    ) -> tuple[bool, str]:
        """Create a new issue.

        Args:
            project_key: The project key.
            summary: Issue summary.
            description: Issue description in ADF format.
            issue_type: Type of issue (default: Task).
            parent_key: Optional parent (Epic) key.
            component: Optional component name.

        Returns:
            Tuple of (success, message/key).
        """
        url = f"{self.base_url}/rest/api/3/issue"

        fields: dict = {
            "project": {"key": project_key},
            "summary": summary,
            "description": description,
            "issuetype": {"name": issue_type},
        }

        if parent_key:
            fields["parent"] = {"key": parent_key}

        if component:
            fields["components"] = [{"name": component}]

        payload = {"fields": fields}

        response = requests.post(url, json=payload, headers=self.headers, auth=self.auth, timeout=30)

        if response.status_code == 201:
            data = response.json()
            return True, data.get("key", "Unknown")

        return False, response.text


def create_jira_tasks(tasks: list[Task], config: dict[str, str | None], dry_run: bool = False) -> None:
    """Create Jira tasks for the given tasks.

    Args:
        tasks: List of Task objects to create.
        config: Jira configuration dictionary.
        dry_run: If True, only print what would be created without actually creating.
    """
    component = config.get("jira_component")

    if dry_run:
        print("=" * 60)
        print("DRY RUN - No tasks will be created")
        print("=" * 60)
        print(f"\nWould create {len(tasks)} tasks in project {config['jira_project']}")
        print(f"Epic: {config['jira_epic_key']}")
        if component:
            print(f"Component: {component}")
        print()

        # Group by section for better readability
        current_section = ""
        for i, task in enumerate(tasks, 1):
            if task.section != current_section:
                current_section = task.section
                print(f"\n--- {current_section} ---")

            print(f"  {i:3}. [{task.resource_name}] {task.description}")

        print()
        return

    # Connect to Jira
    print(f"Connecting to Jira at {config['jira_url']}...")
    client = JiraClient(
        url=config["jira_url"],
        email=config["jira_email"],
        api_token=config["jira_api_token"],
    )

    # Verify the epic exists
    epic = client.get_issue(config["jira_epic_key"])
    if not epic:
        print(f"Error: Could not find epic {config['jira_epic_key']}")
        print("\nSearching for available epics in project...")
        epics = client.search_epics(config["jira_project"])  # type: ignore[arg-type]
        if epics:
            print(f"\nAvailable epics in {config['jira_project']}:")
            for epic_item in epics:
                print(f"  {epic_item['key']:12} [{epic_item['status']:12}] {epic_item['summary']}")
            print("\nSet JIRA_EPIC_KEY to one of the above keys.")
        else:
            print(f"No epics found in project {config['jira_project']}.")
        sys.exit(1)

    epic_summary = epic.get("fields", {}).get("summary", "Unknown")
    print(f"Found epic: {config['jira_epic_key']} - {epic_summary}\n")

    # Create tasks
    created_count = 0
    failed_count = 0

    for task in tasks:
        success, result = client.create_issue(
            project_key=config["jira_project"],  # type: ignore[arg-type]
            summary=task.summary,
            description=task.full_description,
            issue_type="Task",
            parent_key=config["jira_epic_key"],
            component=component,
        )

        if success:
            print(f"✓ Created: {result} - {task.summary}")
            created_count += 1
        else:
            print(f"✗ Failed to create task '{task.summary}':")
            print(f"  {result[:200]}...")  # Truncate long error messages
            failed_count += 1

    print("\n" + "=" * 60)
    print(f"Summary: {created_count} created, {failed_count} failed")
    print("=" * 60)


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Create Jira tasks from uncompleted items in tasks.md",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview tasks without creating them in Jira",
    )
    parser.add_argument(
        "--tasks-file",
        type=Path,
        default=Path(__file__).parent / "tasks.md",
        help="Path to the tasks.md file (default: tasks.md in same directory)",
    )

    args = parser.parse_args()

    # Parse the tasks file
    if not args.tasks_file.exists():
        print(f"Error: Tasks file not found: {args.tasks_file}")
        sys.exit(1)

    print(f"Parsing tasks from: {args.tasks_file}")
    tasks = parse_tasks_file(args.tasks_file)

    if not tasks:
        print("No uncompleted tasks found.")
        return

    print(f"Found {len(tasks)} uncompleted tasks.\n")

    # Get Jira config (skip for dry-run to allow preview without credentials)
    if args.dry_run:
        # Use placeholder config for dry-run
        config: dict[str, str | None] = {
            "jira_url": os.environ.get("JIRA_URL", "https://example.atlassian.net"),
            "jira_email": os.environ.get("JIRA_EMAIL", "user@example.com"),
            "jira_api_token": os.environ.get("JIRA_API_TOKEN", "***"),
            "jira_project": os.environ.get("JIRA_PROJECT", "PROJECT"),
            "jira_epic_key": os.environ.get("JIRA_EPIC_KEY", "EPIC-123"),
            "jira_component": os.environ.get("JIRA_COMPONENT"),
        }
    else:
        config = get_jira_config()

    single_task = tasks[0]

    # Create tasks
    create_jira_tasks([single_task], config, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
