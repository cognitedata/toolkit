from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand


class UploadCommand(ToolkitCommand):
    def upload(
        self,
        client: ToolkitClient,
        build_dir: Path,
        include: list[str] | None = None,
        verbose: bool = False,
    ) -> None:
        raise NotImplementedError()
