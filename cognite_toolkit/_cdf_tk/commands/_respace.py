from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient

from ._base import ToolkitCommand


class RespaceCommand(ToolkitCommand):
    def __init__(
        self,
        print_warning: bool = True,
        skip_tracking: bool = False,
        silent: bool = False,
        client: ToolkitClient | None = None,
    ):
        super().__init__(print_warning, skip_tracking, silent, client)

    def plan(self) -> None:
        """Generate a respace plan from a CSV file."""
        print("[bold yellow]:construction: Work in Progress, you'll be able to plan soon! :construction:[/]")

    def execute(self) -> None:
        """Execute a respace plan."""
        print("[bold yellow]:construction: Work in Progress, you'll be able to execute soon! :construction:[/]")
