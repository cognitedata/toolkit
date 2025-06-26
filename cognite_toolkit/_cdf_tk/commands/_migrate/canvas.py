from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.interactive_select import InteractiveCanvasSelect


class MigrationCanvasCommand(ToolkitCommand):
    def migrate_canvas(
        self,
        client: ToolkitClient,
        external_ids: list[str] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        external_ids = external_ids or InteractiveCanvasSelect(client).select_external_ids()
        print(f"Would migrate {len(external_ids)} canvases: {humanize_collection(external_ids)}")
