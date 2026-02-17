from pathlib import Path

from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel, model_validator
from rich import print

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.storageio._data_classes import ModelList

from ._base import ToolkitCommand


class RespaceMapping(BaseModel, alias_generator=to_camel_case, populate_by_name=True):
    """A single node respace mapping from source to target.

    The external ID is preserved — only the space changes.

    CSV columns: sourceSpace, externalId, targetSpace
    """

    source_space: str
    external_id: str
    target_space: str

    @model_validator(mode="after")
    def _target_differs_from_source(self) -> "RespaceMapping":
        if self.source_space == self.target_space:
            raise ValueError(
                f"targetSpace must differ from sourceSpace, "
                f"but both are '{self.source_space}' for externalId '{self.external_id}'"
            )
        return self


class RespaceMappingList(ModelList[RespaceMapping]):
    @classmethod
    def _get_base_model_cls(cls) -> type[RespaceMapping]:
        return RespaceMapping


class RespaceCommand(ToolkitCommand):
    def __init__(
        self,
        print_warning: bool = True,
        skip_tracking: bool = False,
        silent: bool = False,
        client: ToolkitClient | None = None,
    ):
        super().__init__(print_warning, skip_tracking, silent, client)

    def plan(self, csv_file: Path, output_file: Path) -> None:
        """Generate a respace plan from a CSV file."""
        print(f"[bold]Planning respace from:[/] {csv_file}")
        print(f"[bold]Output plan file:[/] {output_file}")
        print("[bold yellow]:construction: Work in Progress, you'll be able to plan soon! :construction:[/]")

    def execute(self, plan_file: Path, backup_dir: Path, dry_run: bool = False) -> None:
        """Execute a respace plan."""
        verb = "Would execute" if dry_run else "Executing"
        print(f"[bold]{verb} plan:[/] {plan_file}")
        print(f"[bold]Backup directory:[/] {backup_dir}")
        print("[bold yellow]:construction: Work in Progress, you'll be able to execute soon! :construction:[/]")
