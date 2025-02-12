from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient

from ._base import ToolkitCommand


class PopulateCommand(ToolkitCommand):
    def view(
        self,
        client: ToolkitClient,
        view_id: list[str] | None = None,
        table: Path | None = None,
        instance_space: str | None = None,
        external_id_column: str | None = None,
        verbose: bool = False,
    ) -> None:
        raise NotImplementedError()
