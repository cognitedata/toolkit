from __future__ import annotations

import warnings
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator
from functools import lru_cache
from itertools import groupby
from pathlib import Path
from typing import Any, Literal, cast

import pandas as pd
import questionary
from cognite.client.data_classes import Asset, AssetFilter, AssetList, DataSetWrite, DataSetWriteList
from cognite.client.data_classes.filters import Equals
from cognite.client.exceptions import CogniteAPIError
from rich.progress import Progress, TaskID, track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitIsADirectoryError,
    ToolkitMissingResourceError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import DataSetsLoader, LabelLoader
from cognite_toolkit._cdf_tk.loaders._resource_loaders.classic_loaders import AssetLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, yaml_safe_dump


class DumpAssetsCommand(ToolkitCommand):
    # 128 MB
    buffer_size = 128 * 1024 * 1024
    # Note the size in memory is not the same as the size on disk,
    # so the resulting file size will vary.
    encoding = "utf-8"
    newline = "\n"

    def __init__(self, print_warning: bool = True, skip_tracking: bool = False):
        super().__init__(print_warning, skip_tracking)
        self.asset_external_id_by_id: dict[int, str] = {}
        self.data_set_by_id: dict[int, DataSetWrite] = {}
        self._used_labels: set[str] = set()
        self._used_data_sets: set[int] = set()
        self._available_data_sets: dict[int, DataSetWrite] | None = None
        self._available_hierarchies: dict[int, Asset] | None = None
        self._written_files: list[Path] = []
        self._used_columns: set[str] = set()

    def execute(
        self,
        ToolGlobals: CDFToolConfig,
        hierarchy: list[str] | None,
        data_set: list[str] | None,
        output_dir: Path,
        clean: bool,
        limit: int | None = None,
        format_: Literal["yaml", "csv", "parquet"] = "csv",
        verbose: bool = False,
    ) -> None:
        if format_ not in {"yaml", "csv", "parquet"}:
            raise ToolkitValueError(f"Unsupported format {format_}. Supported formats are yaml, csv, parquet.")
        if output_dir.exists() and clean:
            safe_rmtree(output_dir)
        elif output_dir.exists():
            raise ToolkitFileExistsError(f"Output directory {output_dir!s} already exists. Use --clean to remove it.")
        elif output_dir.suffix:
            raise ToolkitIsADirectoryError(f"Output directory {output_dir!s} is not a directory.")
        is_interactive = hierarchy is None and data_set is None
        hierarchies, data_sets = self._select_hierarchy_and_data_set(
            ToolGlobals.toolkit_client, hierarchy, data_set, is_interactive
        )
        if not hierarchies and not data_sets:
            raise ToolkitValueError("No hierarchy or data set provided")

        if missing := set(data_sets) - {item.external_id for item in self.data_set_by_id.values() if item.external_id}:
            try:
                retrieved = ToolGlobals.toolkit_client.data_sets.retrieve_multiple(external_ids=list(missing))
            except CogniteAPIError as e:
                raise ToolkitMissingResourceError(f"Failed to retrieve data sets {data_sets}: {e}")

            self.data_set_by_id.update({item.id: item.as_write() for item in retrieved if item.id})

        (output_dir / AssetLoader.folder_name).mkdir(parents=True, exist_ok=True)

        total_assets = ToolGlobals.toolkit_client.assets.aggregate_count(
            filter=AssetFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )
        if limit:
            total_assets = min(total_assets, limit)

        with Progress() as progress:
            retrieved_assets = progress.add_task("Retrieving assets", total=total_assets)
            write_to_file = progress.add_task("Writing assets to file(s)", total=total_assets)

            asset_iterator = ToolGlobals.toolkit_client.assets(
                chunk_size=1000,
                asset_subtree_external_ids=hierarchies or None,
                data_set_external_ids=data_sets or None,
                limit=limit,
            )
            asset_iterator = self._log_retrieved(asset_iterator, progress, retrieved_assets)
            grouped_assets = self._group_assets(asset_iterator, ToolGlobals.toolkit_client, hierarchies, data_sets)
            writeable = self._to_write(grouped_assets, ToolGlobals.toolkit_client, expand_metadata=True)

            count = 0
            if format_ == "yaml":
                for group, assets in writeable:
                    clean_name = to_directory_compatible(group)
                    file_path = output_dir / AssetLoader.folder_name / f"{clean_name}.Asset.{format_}"
                    if file_path.exists():
                        with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                            f.write("\n")
                            f.write(yaml_safe_dump(assets))
                    else:
                        with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                            f.write(yaml_safe_dump(assets))
                    count += len(assets)
                    progress.advance(write_to_file, advance=len(assets))
            elif format_ in {"csv", "parquet"}:
                file_count_by_hierarchy: dict[str, int] = Counter()
                for group, df in self._buffer(writeable):
                    folder_path = output_dir / AssetLoader.folder_name / to_directory_compatible(group)
                    folder_path.mkdir(parents=True, exist_ok=True)
                    file_count = file_count_by_hierarchy[group]
                    file_path = folder_path / f"part-{file_count:04}.Asset.{format_}"
                    self._used_columns.update(df.columns)
                    # Standardize column order
                    df.sort_index(axis=1, inplace=True)
                    if format_ == "csv":
                        df.to_csv(file_path, index=False, encoding=self.encoding, lineterminator=self.newline)
                    elif format_ == "parquet":
                        df.to_parquet(file_path, index=False)
                    file_count_by_hierarchy[group] += 1
                    if verbose:
                        print(f"Dumped {len(df):,} assets in {group} to {file_path}")
                    count += len(df)
                    self._written_files.append(file_path)
                    progress.advance(write_to_file, advance=len(df))
            else:
                raise ToolkitValueError(f"Unsupported format {format_}. Supported formats are yaml, csv, parquet. ")

        if format_ in {"csv", "parquet"} and len(self._written_files) > 1:
            # Standardize columns across all files
            for file_path in track(
                self._written_files, total=len(self._written_files), description="Standardizing columns"
            ):
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore")
                    if format_ == "csv":
                        df = pd.read_csv(file_path, encoding=self.encoding, lineterminator=self.newline)
                    else:
                        df = pd.read_parquet(file_path)
                for missing_column in self._used_columns - set(df.columns):
                    df[missing_column] = None
                # Standardize column order
                df.sort_index(axis=1, inplace=True)
                if format_ == "csv":
                    df.to_csv(file_path, index=False, encoding=self.encoding, lineterminator=self.newline)
                elif format_ == "parquet":
                    df.to_parquet(file_path, index=False)

        print(f"Dumped {count:,} assets to {output_dir}")

        if self._used_labels:
            labels = ToolGlobals.toolkit_client.labels.retrieve(
                external_id=list(self._used_labels), ignore_unknown_ids=True
            )
            if labels:
                to_dump_dicts = labels.as_write().dump()
                for label in to_dump_dicts:
                    if "dataSetId" in label:
                        data_set_id = label.pop("dataSetId")
                        self._used_data_sets.add(data_set_id)
                        label["dataSetExternalId"] = self._get_data_set_external_id(
                            ToolGlobals.toolkit_client, data_set_id
                        )

                file_path = output_dir / LabelLoader.folder_name / "asset.Label.yaml"
                file_path.parent.mkdir(parents=True, exist_ok=True)
                if file_path.exists():
                    with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                        f.write("\n")
                        f.write(yaml_safe_dump(to_dump_dicts))
                else:
                    with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                        f.write(yaml_safe_dump(to_dump_dicts))

                print(f"Dumped {len(labels):,} labels to {file_path}")

        if self._used_data_sets:
            used_datasets = DataSetWriteList(
                [self.data_set_by_id[used_dataset] for used_dataset in self._used_data_sets]
            )
            to_dump = used_datasets.dump_yaml()
            file_path = output_dir / DataSetsLoader.folder_name / "asset.DataSet.yaml"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if file_path.exists():
                with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                    f.write("\n")
                    f.write(to_dump)
            else:
                with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                    f.write(to_dump)

            print(f"Dumped {len(used_datasets):,} data sets to {file_path}")

    def _buffer(self, asset_iterator: Iterator[tuple[str, list[dict[str, Any]]]]) -> Iterator[tuple[str, pd.DataFrame]]:
        """Iterates over assets util the buffer reaches the filesize."""
        stored_assets: dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)
        for group, assets in asset_iterator:
            stored_assets[group] = pd.concat([stored_assets[group], pd.DataFrame(assets)], ignore_index=True)
            if stored_assets[group].memory_usage().sum() > self.buffer_size:
                yield group, stored_assets.pop(group)
        for group, df in stored_assets.items():
            if not df.empty:
                yield group, df

    @lru_cache
    def get_assets_choice_count_by_dataset(self, item_id: int, client: ToolkitClient) -> int:
        """Using LRU decorator w/o limit instead of another lookup map."""
        return client.assets.aggregate_count(advanced_filter=Equals("dataSetId", item_id))

    @lru_cache
    def get_asset_choice_count_by_root_id(self, item_id: int, client: ToolkitClient) -> int:
        """Using LRU decorator w/o limit instead of another lookup map."""
        return client.assets.aggregate_count(advanced_filter=Equals("rootId", item_id))

    def _create_choice(self, item_id: int, item: Asset | DataSetWrite, client: ToolkitClient) -> questionary.Choice:
        """
        Choice with `title` including name and external_id if they differ.
        Adding `value` as external_id for the choice.
        `item_id` and `item` came in separate as item is DataSetWrite w/o `id`.
        """

        if isinstance(item, DataSetWrite):
            ts_count = self.get_assets_choice_count_by_dataset(item_id, client)
        elif isinstance(item, Asset):
            ts_count = self.get_asset_choice_count_by_root_id(item_id, client)
        else:
            raise TypeError(f"Unsupported item type: {type(item)}")

        return questionary.Choice(
            title=f"{item.name} ({item.external_id}) [{ts_count:,}]"
            if item.name != item.external_id
            else f"({item.external_id}) [{ts_count:,}]",
            value=item.external_id,
        )

    def _get_choice_title(self, choice: str | questionary.Choice | dict[str, Any]) -> str:
        """
        Accommodates for the fact that the choice, string or a dict, when `sorted(key=..)` is called.
        So assert confirm the type of the choice and choice.title and then return the title.
        """
        assert isinstance(choice, questionary.Choice)
        assert isinstance(choice.title, str)  # minimum external_id is set
        return choice.title.casefold()  # superior to lower case like `ß>ss` in German

    def _select_hierarchy_and_data_set(
        self, client: ToolkitClient, hierarchy: list[str] | None, data_set: list[str] | None, interactive: bool
    ) -> tuple[list[str], list[str]]:
        if not interactive:
            return hierarchy or [], data_set or []

        hierarchies: set[str] = set()
        data_sets: set[str] = set()
        while True:
            selected = []
            if hierarchies:
                selected.append(f"Selected hierarchies: {sorted(hierarchies)}")
            else:
                selected.append("No hierarchy selected.")
            if data_sets:
                selected.append(f"Selected data sets: {sorted(data_sets)}")
            else:
                selected.append("No data set selected.")
            selected_str = "\n".join(selected)
            what = questionary.select(
                f"\n{selected_str}\nSelect a hierarchy or data set to dump",
                choices=["Hierarchy", "Data Set", "Done", "Abort"],
            ).ask()

            if what == "Done":
                break
            elif what == "Abort":
                return [], []
            elif what == "Hierarchy":
                _available_hierarchies = self._get_available_hierarchies(client)
                selected_hierarchy = questionary.checkbox(
                    "Select a hierarchy listed as 'name (external_id) [count]'",
                    choices=sorted(
                        [
                            self._create_choice(item_id, item, client)
                            for (item_id, item) in _available_hierarchies.items()
                            if item.external_id not in hierarchies
                        ],
                        key=self._get_choice_title,
                        # sorted cannot find the proper overload
                    ),  # type: ignore
                ).ask()
                if selected_hierarchy:
                    hierarchies.update(selected_hierarchy)
                else:
                    print("No hierarchy selected.")
            elif what == "Data Set":
                _available_data_sets = self._get_available_data_sets(client)
                selected_data_set = questionary.checkbox(
                    "Select a data set listed as 'name (external_id) [count]'",
                    choices=sorted(
                        [
                            self._create_choice(item_id, item, client)
                            for (item_id, item) in _available_data_sets.items()
                            if item.external_id not in data_sets
                        ],
                        # sorted cannot find the proper overload
                        key=self._get_choice_title,
                    ),  # type: ignore
                ).ask()
                if selected_data_set:
                    data_sets.update(selected_data_set)
                else:
                    print("No data set selected.")
        return list(hierarchies), list(data_sets)

    def _get_available_data_sets(self, client: ToolkitClient) -> dict[int, DataSetWrite]:
        if self._available_data_sets is None:
            self.data_set_by_id.update({item.id: item.as_write() for item in client.data_sets})
            # filter out data sets without external_id
            self._available_data_sets = {
                item_id: item for (item_id, item) in self.data_set_by_id.items() if item.external_id
            }
        return self._available_data_sets

    def _get_available_hierarchies(self, client: ToolkitClient) -> dict[int, Asset]:
        if self._available_hierarchies is None:
            self._available_hierarchies = {}
            for item in client.assets(root=True):
                if item.id and item.external_id:
                    self.asset_external_id_by_id[item.id] = item.external_id
                if item.external_id:
                    self._available_hierarchies.update({item.id: item})
        return self._available_hierarchies

    def _group_assets(
        self,
        assets: Iterator[AssetList],
        client: ToolkitClient,
        hierarchies: list[str] | None,
        data_sets: list[str] | None,
    ) -> Iterator[tuple[str, AssetList]]:
        key: Callable[[Asset], int | tuple[int, int]]
        lookup: Callable[[ToolkitClient, int | tuple[int, int]], str]

        if hierarchies and data_sets:

            def key(a: Asset) -> tuple[int, int]:
                return a.root_id or 0, a.data_set_id or 0

            def lookup(c: ToolkitClient, group: tuple[int, int]) -> str:  # type: ignore[misc]
                return f"{self._get_asset_external_id(c, group[0])}.{self._get_data_set_external_id(c, group[1])}"
        elif hierarchies and not data_sets:

            def key(a: Asset) -> int:
                return a.root_id or 0

            lookup = self._get_asset_external_id  # type: ignore[assignment]

        else:  # data_sets and not hierarchies:

            def key(a: Asset) -> int:
                return a.data_set_id or 0

            lookup = self._get_data_set_external_id  # type: ignore[assignment]

        for asset_list in assets:
            for group, hierarchy_asset in groupby(sorted(asset_list, key=key), key=key):
                yield lookup(client, group), AssetList(list(hierarchy_asset))

    def _to_write(
        self, assets: Iterator[tuple[str, AssetList]], client: ToolkitClient, expand_metadata: bool
    ) -> Iterator[tuple[str, list[dict[str, Any]]]]:
        for group, asset_list in assets:
            write_assets: list[dict[str, Any]] = []
            for asset in asset_list:
                write = asset.as_write().dump(camel_case=True)
                write.pop("parentId", None)
                if "dataSetId" in write:
                    data_set_id = write.pop("dataSetId")
                    self._used_data_sets.add(data_set_id)
                    write["dataSetExternalId"] = self._get_data_set_external_id(client, data_set_id)
                if expand_metadata and "metadata" in write:
                    metadata = write.pop("metadata")
                    for key, value in metadata.items():
                        write[f"metadata.{key}"] = value
                if "rootId" in write:
                    root_id = write.pop("rootId")
                    write["rootExternalId"] = self._get_asset_external_id(client, root_id)
                if isinstance(write.get("labels"), list):
                    write["labels"] = [label["externalId"] for label in write["labels"]]
                    self._used_labels.update(write["labels"])
                write_assets.append(write)
            yield group, write_assets

    def _get_asset_external_id(self, client: ToolkitClient, root_id: int) -> str:
        if root_id in self.asset_external_id_by_id:
            return self.asset_external_id_by_id[root_id]
        try:
            asset = client.assets.retrieve(id=root_id)
        except CogniteAPIError as e:
            raise ToolkitMissingResourceError(f"Failed to retrieve asset {root_id}: {e}")
        if asset is None:
            raise ToolkitMissingResourceError(f"Asset {root_id} does not exist")
        if not asset.external_id:
            raise ToolkitValueError(f"Asset {root_id} does not have an external id")
        self.asset_external_id_by_id[root_id] = asset.external_id
        return asset.external_id

    def _get_data_set_external_id(self, client: ToolkitClient, data_set_id: int) -> str:
        if data_set_id in self.data_set_by_id:
            return cast(str, self.data_set_by_id[data_set_id].external_id)
        try:
            data_set = client.data_sets.retrieve(id=data_set_id)
        except CogniteAPIError as e:
            raise ToolkitMissingResourceError(f"Failed to retrieve data set {data_set_id}: {e}")
        if data_set is None:
            raise ToolkitMissingResourceError(f"Data set {data_set_id} does not exist")
        if not data_set.external_id:
            raise ToolkitValueError(f"Data set {data_set_id} does not have an external id")
        self.data_set_by_id[data_set_id] = data_set.as_write()
        return data_set.external_id

    @staticmethod
    def _log_retrieved(asset_iterator: Iterator[AssetList], progress: Progress, task: TaskID) -> Iterator[AssetList]:
        for asset_list in asset_iterator:
            progress.advance(task, advance=len(asset_list))
            yield asset_list
