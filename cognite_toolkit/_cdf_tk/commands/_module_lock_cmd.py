from datetime import UTC, datetime

import questionary
from rich import print
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.deploy_v2.module_lock import SPACE, VIEW_ID, ModuleLock
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitValueError


class ModuleLockCommand(ToolkitCommand):
    """Command to inspect and release module locks stored in CDF."""

    def locks(self, client: ToolkitClient, unlock: bool = True, dry_run: bool = False) -> list[ModuleLock]:
        """List all module locks with their status and optionally let the user release active locks.

        Args:
            client: The ToolkitClient to use.
            unlock: Whether to prompt the user to unlock active locks.
            dry_run: If True, no locks are released.

        Returns:
            The list of locks that were released (or would be released in dry-run mode).
        """
        locks = self._list_locks(client)
        if locks is None:
            print(
                "[yellow]Module locking has not been set up in this CDF project[/yellow] "
                f"(the view {VIEW_ID!s} is not deployed). No modules are locked.\n"
                "The view is created automatically the first time modules are locked during deploy."
            )
            return []

        email_by_user_id = self._lookup_emails(client, locks)
        active_locks = [lock for lock in locks if self._is_active(lock)]
        inactive_locks = [lock for lock in locks if not self._is_active(lock)]

        self._display_locks(active_locks, inactive_locks, email_by_user_id)

        if not unlock:
            return []
        if not active_locks:
            print("No active locks to release.")
            return []

        choices = [
            questionary.Choice(
                title=(
                    f"{lock.module_id} (repo: {lock.repo or 'unknown'}, branch: {lock.branch}, "
                    f"by: {self._display_user(lock, email_by_user_id)}, {self._humanize_age(lock.timestamp)})"
                ),
                value=lock,
            )
            for lock in active_locks
        ]
        choices.append(
            questionary.Choice(
                title="Cancel (do not release any locks)",
                value="cancel",
            )
        )

        selected: list[ModuleLock] | None = questionary.checkbox(
            "Select the module locks you want to release:",
            choices=choices,
        ).unsafe_ask()

        if not selected or "cancel" in selected:
            print("Operation cancelled.")
            return []

        if dry_run:
            print(f"Would release {len(selected)} lock(s): {', '.join(lock.module_id for lock in selected)}")
            return selected

        if not questionary.confirm(
            f"Are you sure you want to release {len(selected)} lock(s)?", default=False
        ).unsafe_ask():
            print("Aborted.")
            return []

        released = [lock.model_copy(update={"lock": False, "timestamp": datetime.now(UTC)}) for lock in selected]
        try:
            client.instances.create(ModuleLock.as_nodes(released), replace=True)
        except ToolkitAPIError as e:
            raise ToolkitValueError(f"Failed to release module locks due to API error: {e.code} - {e.message}") from e
        print(f"Released {len(released)} lock(s): {', '.join(lock.module_id for lock in released)}")
        return released

    @staticmethod
    def _list_locks(client: ToolkitClient) -> list[ModuleLock] | None:
        try:
            nodes = client.instances.list(
                filter=InstanceFilter(instance_type="node", source=VIEW_ID, space=[SPACE]), limit=None
            )
        except ToolkitAPIError as e:
            # The error message is not consistent, its seems sometimes "not exist" and sometimes "unknown" is used. So we check for both.
            if (
                e.code is not None
                and 400 <= e.code < 500
                and ("not exist" in e.message.lower() or "unknown" in e.message.lower())
            ):
                # The view does not exist, which means module locking has not been set up yet.
                return None
            raise ToolkitValueError(f"Failed to retrieve module locks due to API error: {e.code} - {e.message}") from e
        return sorted(ModuleLock.from_nodes(nodes), key=lambda lock: (lock.repo or "", lock.module_id))

    @staticmethod
    def _is_active(lock: ModuleLock) -> bool:
        return lock.lock and not lock.is_expired

    @staticmethod
    def _lookup_emails(client: ToolkitClient, locks: list[ModuleLock]) -> dict[str, str]:
        """Look up the emails of the users holding the locks. Failures (e.g. missing access) are ignored."""
        email_by_user_id: dict[str, str] = {}
        for user_id in dict.fromkeys(lock.user_id for lock in locks):
            try:
                email = client.lookup.users.email(user_id, warn=False)
            except (ToolkitAPIError, ToolkitError):
                # Typically missing access to list users; fall back to showing the user ID.
                break
            if email:
                email_by_user_id[user_id] = email
        return email_by_user_id

    @staticmethod
    def _display_user(lock: ModuleLock, email_by_user_id: dict[str, str]) -> str:
        email = email_by_user_id.get(lock.user_id)
        return f"{lock.git_name} <{email}>" if email else f"{lock.git_name} ({lock.user_id})"

    @staticmethod
    def _humanize_age(timestamp: datetime) -> str:
        seconds = int((datetime.now(UTC) - timestamp).total_seconds())
        if seconds < 60:
            return "just now"
        for unit_seconds, unit in ((86400, "d"), (3600, "h"), (60, "m")):
            if seconds >= unit_seconds:
                return f"{seconds // unit_seconds}{unit} ago"
        return "just now"

    @staticmethod
    def _status(lock: ModuleLock) -> str:
        if not lock.lock:
            return "[green]Released[/green]"
        if lock.is_expired:
            return "[yellow]Expired[/yellow]"
        return "[red]Locked[/red]"

    def _create_table(self, title: str, locks: list[ModuleLock], email_by_user_id: dict[str, str]) -> Table:
        table = Table(title=title, title_justify="left", show_lines=False, expand=False)
        table.add_column("#", justify="right", style="dim")
        table.add_column("Module", style="bold cyan")
        table.add_column("Status")
        table.add_column("Repository", style="blue")
        table.add_column("Branch", style="magenta")
        table.add_column("User")
        table.add_column("Last updated", justify="right")
        for no, lock in enumerate(locks, 1):
            table.add_row(
                str(no),
                lock.module_id,
                self._status(lock),
                lock.repo or "[dim]unknown[/dim]",
                lock.branch,
                self._display_user(lock, email_by_user_id),
                f"{self._humanize_age(lock.timestamp)}\n[dim]{lock.timestamp.astimezone(UTC):%Y-%m-%d %H:%M} UTC[/dim]",
            )
        return table

    def _display_locks(
        self, active: list[ModuleLock], inactive: list[ModuleLock], email_by_user_id: dict[str, str]
    ) -> None:
        print(
            f"Found [bold]{len(active) + len(inactive)}[/bold] module lock(s): "
            f"[red]{len(active)} active[/red], [green]{len(inactive)} inactive[/green]\n"
        )
        if active:
            print(self._create_table("[bold red]Active locks[/bold red]", active, email_by_user_id))
        else:
            print("[green]No active locks.[/green]")
        print()
        if inactive:
            print(
                self._create_table(
                    "[bold green]Inactive locks (released or expired)[/bold green]", inactive, email_by_user_id
                )
            )
        else:
            print("[dim]No inactive locks.[/dim]")
        print()
