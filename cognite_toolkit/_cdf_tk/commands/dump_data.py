import warnings
from abc import abstractmethod
from collections import Counter, defaultdict
from collections.abc import Hashable, Iterator
from functools import lru_cache, partial
from itertools import groupby
from pathlib import Path
from typing import Any, Generic, Literal

import pandas as pd
import questionary
from cognite.client.data_classes import (
    Asset,
    AssetFilter,
    AssetList,
    DataSet,
    DataSetList,
    TimeSeries,
    TimeSeriesFilter,
    TimeSeriesList,
)
from cognite.client.data_classes._base import T_CogniteResource, T_CogniteResourceList
from rich.progress import Progress, TaskID, track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitIsADirectoryError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import AssetLoader, DataSetsLoader, LabelLoader, ResourceLoader, TimeSeriesLoader
from cognite_toolkit._cdf_tk.utils import to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, yaml_safe_dump


class DataFinder(Generic[T_CogniteResource, T_CogniteResourceList]):
    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
        self.loader = self._create_loader(client)

    @abstractmethod
    def _create_loader(self, client: ToolkitClient) -> ResourceLoader:
        """Create the appropriate loader for the finder."""
        raise NotImplementedError()

    def key(self, item: T_CogniteResource, hierarchies: list[str], data_sets: list[str]) -> str:
        """Return the key for grouping items. Default is empty string."""
        return ""

    @lru_cache
    def aggregate_count(self, hierarchies: tuple[str, ...], data_sets: tuple[str, ...]) -> int:
        return self._aggregate_count(list(hierarchies), list(data_sets))

    @abstractmethod
    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        raise NotImplementedError()

    @abstractmethod
    def create_iterator(
        self, hierarchies: list[str], data_sets: list[str], limit: int | None
    ) -> Iterator[T_CogniteResourceList]:
        raise NotImplementedError()


class AssetFinder(DataFinder[Asset, AssetList]):
    def _create_loader(self, client: ToolkitClient) -> ResourceLoader:
        return AssetLoader.create_loader(client)

    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self.client.assets.aggregate_count(
            filter=AssetFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )

    def create_iterator(self, hierarchies: list[str], data_sets: list[str], limit: int | None) -> Iterator[AssetList]:
        return self.client.assets(
            chunk_size=1000,
            asset_subtree_external_ids=hierarchies or None,
            data_set_external_ids=data_sets or None,
            limit=limit,
        )

    def key(self, item: Asset, hierarchies: list[str], data_sets: list[str]) -> str:
        if hierarchies and data_sets:
            asset_external_id = self.client.lookup.assets.external_id(item.root_id or 0)
            data_set_external_id = self.client.lookup.data_sets.external_id(item.data_set_id or 0)
            if asset_external_id and data_set_external_id:
                return f"{asset_external_id}.{data_set_external_id}"
            elif asset_external_id:
                return asset_external_id
            elif data_set_external_id:
                return data_set_external_id
            return ""
        elif hierarchies:
            return self.client.lookup.assets.external_id(item.root_id or 0) or ""
        elif data_sets:
            return self.client.lookup.data_sets.external_id(item.data_set_id or 0) or ""
        return ""


class TimeSeriesFinder(DataFinder[TimeSeries, TimeSeriesList]):
    def _create_loader(self, client: ToolkitClient) -> ResourceLoader:
        return TimeSeriesLoader.create_loader(client)

    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self.client.time_series.aggregate_count(
            filter=TimeSeriesFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )

    def create_iterator(
        self, hierarchies: list[str], data_sets: list[str], limit: int | None
    ) -> Iterator[TimeSeriesList]:
        return self.client.time_series(
            chunk_size=1000,
            data_set_external_ids=data_sets or None,
            asset_subtree_external_ids=hierarchies or None,
            limit=limit,
        )


class DumpDataCommand(ToolkitCommand):
    # 128 MB
    buffer_size = 128 * 1024 * 1024
    # Note the size in memory is not the same as the size on disk,
    # so the resulting file size will vary.
    encoding = "utf-8"
    newline = "\n"

    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)
        self._used_labels: set[str] = set()
        self._used_data_sets: set[str] = set()
        self._written_files: list[Path] = []
        self._used_columns: set[str] = set()

    def dump_table(
        self,
        finder: DataFinder,
        user_hierarchy: list[str] | None,
        user_data_set: list[str] | None,
        output_dir: Path,
        clean: bool,
        limit: int | None = None,
        format_: Literal["yaml", "csv", "parquet"] = "csv",
        verbose: bool = False,
    ) -> None:
        """Dumps data from CDF to a file

        Args:
            finder (DataFinder): The finder object to use for fetching data.
            user_hierarchy (list[str] | None): The list of hierarchy external IDs to dump.
            user_data_set (list[str] | None): The list of data set external IDs to dump.
            output_dir (Path): The directory to write the output files to.
            clean (bool): Whether to clean the output directory before writing files.
            limit (int | None, optional): The maximum number of rows to write. Defaults to None.
            format_ (Literal["yaml", "csv", "parquet"], optional): The format of the output file. Defaults to "csv".
            verbose (bool, optional): Whether to print detailed progress information. Defaults to False.
        """
        if format_ not in {"yaml", "csv", "parquet"}:
            raise ToolkitValueError(f"Unsupported format {format_}. Supported formats are yaml, csv, parquet.")
        if output_dir.exists() and clean:
            safe_rmtree(output_dir)
        elif output_dir.exists():
            raise ToolkitFileExistsError(f"Output directory {output_dir!s} already exists. Use --clean to remove it.")
        elif output_dir.suffix:
            raise ToolkitIsADirectoryError(f"Output directory {output_dir!s} is not a directory.")

        hierarchies, data_sets = self._select_hierarchy_datasets(user_hierarchy, user_data_set, finder)
        if not hierarchies and not data_sets:
            raise ToolkitValueError("No hierarchy or data set provided")

        total_items = finder.aggregate_count(tuple(hierarchies), tuple(data_sets))
        if limit:
            total_items = min(total_items, limit)

        loader = finder.loader
        with Progress() as progress:
            retrieved_time_series = progress.add_task(f"Retrieving {loader.display_name}", total=total_items)
            write_to_file = progress.add_task(f"Writing {loader.display_name} to file(s)", total=total_items)

            item_iterator = finder.create_iterator(hierarchies, data_sets, limit)
            item_iterator = self._log_retrieved(item_iterator, progress, retrieved_time_series)
            grouped_items = self._group_items(item_iterator, finder, hierarchies, data_sets)
            writeable = self._to_write(grouped_items, loader, expand_metadata=True)
            count = 0
            if format_ == "yaml":
                for group, item_list in writeable:
                    clean_name = to_directory_compatible(group) if group else "my"
                    file_path = output_dir / loader.folder_name / f"{clean_name}.{loader.kind}.yaml"
                    self._write_to_yaml(item_list, file_path)
                    count += len(item_list)
                    progress.advance(write_to_file, advance=len(item_list))
            elif format_ in {"csv", "parquet"}:
                file_count_by_group: dict[str, int] = Counter()
                for group, df in self._table_buffer(writeable):
                    folder_path = output_dir / loader.folder_name
                    if group != "":
                        folder_path /= to_directory_compatible(group)
                    folder_path.mkdir(parents=True, exist_ok=True)
                    file_count = file_count_by_group[group]
                    file_path = folder_path / f"part-{file_count:04}.{loader.kind}.{format_}"
                    self._used_columns.update(df.columns)
                    # Standardize column order
                    df.sort_index(axis=1, inplace=True)

                    if format_ == "csv":
                        df.to_csv(file_path, index=False, encoding=self.encoding, lineterminator=self.newline)
                    elif format_ == "parquet":
                        df.to_parquet(file_path, index=False)
                    if verbose:
                        print(f"Dumped {len(df):,} {loader.display_name} in {group} to {file_path}")
                    self._written_files.append(file_path)
                    file_count_by_group[group] += 1
                    count += len(df)
                    progress.advance(write_to_file, advance=len(df))
            else:
                raise ToolkitValueError(f"Unsupported format {format_}. Supported formats are yaml, csv, parquet. ")

        if format_ in {"csv", "parquet"} and len(self._written_files) > 1:
            self._standardize_columns(format_)

        print(f"Dumped {count:,} assets to {output_dir}")

        if self._used_labels:
            self._write_labels(finder.client, output_dir, loader.kind.casefold())

        if self._used_data_sets:
            self._write_data_sets(finder.client, output_dir, loader.kind.casefold())

    def _select_hierarchy_datasets(
        self, user_hierarchy: list[str] | None, user_data_set: list[str] | None, finder: DataFinder
    ) -> tuple[list[str], list[str]]:
        if user_hierarchy or user_data_set:
            return user_hierarchy or [], user_data_set or []
        return self.interactive_select_hierarchy_datasets(finder)

    def interactive_select_hierarchy_datasets(self, finder: DataFinder) -> tuple[list[str], list[str]]:
        """Interactively select hierarchies and data sets to dump."""
        hierarchies: set[str] = set()
        data_sets: set[str] = set()
        while True:
            selected: list[str] = []
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
                options = [
                    asset
                    for asset in self._get_available_hierarchies(finder.client)
                    if asset.external_id not in hierarchies
                ]
                selected_hierarchy = self._select(what, options, finder)
                if selected_hierarchy:
                    hierarchies.update(selected_hierarchy)
                else:
                    print("No hierarchy selected.")
            elif what == "Data Set":
                available_data_sets = [
                    data_set
                    for data_set in self._get_available_data_sets(finder.client)
                    if data_set.external_id not in data_sets
                ]
                selected_data_set = self._select(what, available_data_sets, finder)
                if selected_data_set:
                    data_sets.update(selected_data_set)
                else:
                    print("No data set selected.")
        return list(hierarchies), list(data_sets)

    def _select(self, what: str, options: list[Asset] | list[DataSet], finder: DataFinder) -> str | None:
        return questionary.checkbox(
            f"Select a {what} listed as 'name (external_id) [count]'",
            choices=[
                choice
                for choice, count in (
                    # MyPy does not seem to understand that item is Asset | DataSet
                    self._create_choice(item, finder)  # type: ignore[arg-type]
                    for item in sorted(options, key=lambda x: x.name or x.external_id)
                )
                if count > 0
            ],
        ).ask()

    @staticmethod
    def _create_choice(item: Asset | DataSet, finder: DataFinder) -> tuple[questionary.Choice, int]:
        """
        Choice with `title` including name and external_id if they differ.
        Adding `value` as external_id for the choice.
        `item_id` and `item` came in separate as item is DataSetWrite w/o `id`.
        """

        if isinstance(item, DataSet):
            if item.external_id is None:
                raise ValueError(f"Missing external ID for DataSet {item.id}")
            item_count = finder.aggregate_count(tuple(), (item.external_id,))
        elif isinstance(item, Asset):
            if item.external_id is None:
                raise ValueError(f"Missing external ID for Asset {item.id}")
            item_count = finder.aggregate_count((item.external_id,), tuple())
        else:
            raise TypeError(f"Unsupported item type: {type(item)}")

        return questionary.Choice(
            title=f"{item.name} ({item.external_id}) [{item_count:,}]"
            if item.name != item.external_id
            else f"({item.external_id}) [{item_count:,}]",
            value=item.external_id,
        ), item_count

    def _write_data_sets(self, client: ToolkitClient, output_dir: Path, item_name: str) -> None:
        self._write_items_to_yaml(
            DataSetsLoader.create_loader(client), list(self._used_data_sets), output_dir, item_name
        )

    def _write_labels(self, client: ToolkitClient, output_dir: Path, item_name: str) -> None:
        self._write_items_to_yaml(LabelLoader.create_loader(client), list(self._used_labels), output_dir, item_name)

    def _write_items_to_yaml(
        self, loader: ResourceLoader, ids: list[Hashable], output_dir: Path, item_name: str
    ) -> None:
        if not (items := loader.retrieve(ids)):
            return
        to_dump = [loader.dump_resource(item) for item in items]
        file_path = output_dir / loader.folder_name / f"{item_name}.{loader.kind}.yaml"
        self._write_to_yaml(to_dump, file_path)
        print(f"Dumped {len(items):,} {loader.display_name} to {file_path}")

    def _standardize_columns(self, format_: Literal["yaml", "csv", "parquet"]) -> None:
        """Standardizes the columns across all written files to ensure consistency.

        This is required when loading the files into tools such as DuckDB.
        """
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

    def _write_to_yaml(self, item_list: list[dict], file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.exists():
            with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                f.write("\n")
                f.write(yaml_safe_dump(item_list))
        else:
            with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                f.write(yaml_safe_dump(item_list))

    @staticmethod
    def _log_retrieved(
        item_iterator: Iterator[T_CogniteResourceList], progress: Progress, task: TaskID
    ) -> Iterator[T_CogniteResourceList]:
        for item_list in item_iterator:
            progress.advance(task, advance=len(item_list))
            yield item_list

    @staticmethod
    def _group_items(
        items: Iterator[T_CogniteResourceList],
        finder: DataFinder[T_CogniteResource, T_CogniteResourceList],
        hierarchies: list[str],
        data_sets: list[str],
    ) -> Iterator[tuple[str, T_CogniteResourceList]]:
        for item_list in items:
            key = partial(finder.key, hierarchies=hierarchies, data_sets=data_sets)
            for group, grouped_items in groupby(sorted(item_list, key=key), key=key):
                yield group, finder.loader.list_cls(list(grouped_items))

    def _to_write(
        self, items: Iterator[tuple[str, T_CogniteResourceList]], loader: ResourceLoader, expand_metadata: bool
    ) -> Iterator[tuple[str, list[dict[str, Any]]]]:
        for group, item_list in items:
            write_item: list[dict[str, Any]] = []
            for item in item_list:
                dumped = loader.dump_resource(item)
                if expand_metadata and "metadata" in dumped:
                    metadata = dumped.pop("metadata")
                    for key, value in metadata.items():
                        dumped[f"metadata.{key}"] = value
                if isinstance(dumped.get("labels"), list):
                    dumped["labels"] = [label["externalId"] for label in dumped["labels"]]
                    self._used_labels.update(dumped["labels"])
                if "dataSetExternalId" in dumped:
                    self._used_data_sets.add(dumped["dataSetExternalId"])
                write_item.append(dumped)
            yield group, write_item

    def _table_buffer(
        self, item_iterator: Iterator[tuple[str, list[dict[str, Any]]]]
    ) -> Iterator[tuple[str, pd.DataFrame]]:
        stored_items: dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)
        for group, assets in item_iterator:
            stored_items[group] = pd.concat([stored_items[group], pd.DataFrame(assets)], ignore_index=True)
            if stored_items[group].memory_usage().sum() > self.buffer_size:
                yield group, stored_items.pop(group)
        for group, df in stored_items.items():
            if not df.empty:
                yield group, df

    @lru_cache
    def _get_available_data_sets(self, client: ToolkitClient) -> DataSetList:
        return client.data_sets.list(limit=-1)

    @lru_cache
    def _get_available_hierarchies(self, client: ToolkitClient) -> AssetList:
        return client.assets.list(root=True, limit=-1)
