from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.interactive_select import InteractiveCanvasSelection


class MigrationCanvasCommand(ToolkitCommand):
    def migrate_canvas(
        self,
        client: ToolkitClient,
        names: list[str] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        names = names or InteractiveCanvasSelection(client).select_external_ids()
        print(f"Would migrate {len(names)} canvases: {humanize_collection(names)}")
