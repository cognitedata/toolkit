import shutil
from pathlib import Path

import questionary
from cognite.client import CogniteClient
from cognite.client.data_classes import AssetList, AssetWriteList, DataSetWrite
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitIsADirectoryError,
    ToolkitMissingResourceError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import DataSetsLoader
from cognite_toolkit._cdf_tk.prototypes.resource_loaders import AssetLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class DumpAssetsCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, user_command: str | None = None, skip_tracking: bool = False):
        super().__init__(print_warning, user_command, skip_tracking)
        self.asset_external_id_by_id: dict[int, str] = {}
        self.data_set_external_id_by_id: dict[int, DataSetWrite] = {}
        self._available_data_sets: set[str] | None = None
        self._available_hierarchies: set[str] | None = None

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
        if not output_dir.is_dir():
            raise ToolkitIsADirectoryError(f"Output directory {output_dir!s} is not a directory.")
        elif output_dir.exists() and clean:
            shutil.rmtree(output_dir)
        elif output_dir.exists():
            raise ToolkitFileExistsError(f"Output directory {output_dir!s} already exists. Use --clean to remove it.")

        (output_dir / AssetLoader.folder_name).mkdir(parents=True, exist_ok=True)
        (output_dir / DataSetsLoader.folder_name).mkdir(parents=True, exist_ok=True)

        hierarchies, data_sets = self._select_hierarchy_and_data_set(
            ToolGlobals.client, hierarchy, data_set, interactive
        )
        if not hierarchies and not data_sets:
            raise ToolkitValueError("No hierarchy or data set provided")

        if missing := set(data_sets) - {
            item.external_id for item in self.data_set_external_id_by_id.values() if item.external_id
        }:
            try:
                retrieved = ToolGlobals.client.data_sets.retrieve_multiple(external_ids=list(missing))
            except CogniteAPIError as e:
                raise ToolkitMissingResourceError(f"Failed to retrieve data sets {data_sets}: {e}")

            self.data_set_external_id_by_id.update({item.id: item.as_write() for item in retrieved if item.id})

        for assets in ToolGlobals.client.assets(
            chunk_size=1000, asset_subtree_external_ids=hierarchies, data_set_external_ids=data_set
        ):
            for group_name, group in self._group_assets(assets):
                group_write = self._to_write(ToolGlobals.client, group)

                file_path = output_dir / AssetLoader.folder_name / f"{group_name}.yaml"
                file_path.write_text(group_write.dump_yaml())

    def _select_hierarchy_and_data_set(
        self, client: CogniteClient, hierarchy: list[str] | None, data_set: list[str] | None, interactive: bool
    ) -> tuple[list[str], list[str]]:
        if not interactive:
            return hierarchy or [], data_set or []

        hierarchies: set[str] = set()
        data_sets: set[str] = set()
        while True:
            what = questionary.select(
                "Select a hierarchy or data set to dump",
                choices=[
                    "Hierarchy",
                    "Data Set",
                    "Done",
                ],
            ).ask()

            if what == "Done":
                break
            elif what == "Hierarchy":
                _available_hierarchies = self._get_available_hierarchies(client)
                hierarchies.add(
                    questionary.autocomplete(
                        "Select a hierarchy",
                        choices=sorted(item for item in _available_hierarchies if item not in hierarchies),
                    ).ask()
                )
            elif what == "Data Set":
                _available_data_sets = self._get_available_data_sets(client)
                data_sets.add(
                    questionary.autocomplete(
                        "Select a data set",
                        choices=sorted(item for item in _available_data_sets if item not in data_sets),
                    ).ask()
                )
        return list(hierarchies), list(data_sets)

    def _get_available_data_sets(self, client: CogniteClient) -> set[str]:
        if self._available_data_sets is None:
            self.data_set_external_id_by_id.update({item.id: item.as_write() for item in client.data_sets})
            self._available_data_sets = {
                item.external_id for item in self.data_set_external_id_by_id.values() if item.external_id
            }
        return self._available_data_sets

    def _get_available_hierarchies(self, client: CogniteClient) -> set[str]:
        if self._available_hierarchies is None:
            self._available_hierarchies = {item.external_id for item in client.assets(root=True) if item.external_id}
        return self._available_hierarchies

    def _group_assets(self, assets: AssetList) -> tuple[str, AssetList]:
        raise NotImplementedError()

    def _to_write(self, client: CogniteClient, assets: AssetList) -> AssetWriteList:
        raise NotImplementedError()
