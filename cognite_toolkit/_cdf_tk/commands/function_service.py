from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand


class FunctionServiceCommand(ToolkitCommand):
    def activate(self, client: ToolkitClient, dry_run: bool = False) -> str | None:
        """Activate the CDF Function service in the project."""
        from cognite_toolkit._cdf_tk.commands.auth import AuthCommand

        auth = AuthCommand(
            client=client,
            print_warning=self._print_warning,
            skip_tracking=not self.tracker.skip_tracking,
        )
        return auth.activate_function_service(client, dry_run=dry_run)
