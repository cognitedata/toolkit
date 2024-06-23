from pathlib import Path

from cognite.client import CogniteClient
from cognite.client.data_classes import AssetList, AssetWriteList

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.prototypes.resource_loaders import AssetLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class DumpAssetsCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, user_command: str | None = None, skip_tracking: bool = False):
        super().__init__(print_warning, user_command, skip_tracking)
        self.asset_external_id_by_id: dict[int, str] = {}

    def execute(
        self,
        ToolGlobals: CDFToolConfig,
        hierarchy: list[str] | None,
        data_set: list[str] | None,
        interactive: bool,
        output_dir: Path,
        clean: bool,
        verbose: bool = False,
    ) -> None:
        hierarchy, data_set = self._select_hierarchy_and_data_set(ToolGlobals.client, hierarchy, data_set, interactive)

        for assets in ToolGlobals.client.assets(
            chunk_size=1000, asset_subtree_external_ids=hierarchy, data_set_external_ids=data_set
        ):
            self.asset_external_id_by_id.update(
                {asset.id: asset.external_id for asset in assets if asset.external_id and asset.id}
            )
            for group_name, group in self._group_assets(assets):
                group_write = self._to_write(ToolGlobals.client, group)

                file_path = output_dir / AssetLoader.folder_name / f"{group_name}.yaml"
                file_path.write_text(group_write.dump_yaml())

    def _select_hierarchy_and_data_set(
        self, client: CogniteClient, hierarchy: list[str] | None, data_set: list[str] | None, interactive: bool
    ) -> tuple[list[str], list[str]]:
        raise NotImplementedError()

    def _group_assets(self, assets: AssetList) -> tuple[str, AssetList]:
        raise NotImplementedError()

    def _to_write(self, client: CogniteClient, assets: AssetList) -> AssetWriteList:
        raise NotImplementedError()
