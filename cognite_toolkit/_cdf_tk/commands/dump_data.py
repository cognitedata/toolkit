import warnings
from abc import abstractmethod
from collections import Counter, defaultdict
from collections.abc import Iterator
from itertools import groupby
from pathlib import Path
from typing import Any, Generic, Literal

import pandas as pd
from cognite.client.data_classes import Asset, AssetFilter, AssetList
from cognite.client.data_classes._base import T_CogniteResource, T_CogniteResourceList
from rich.progress import Progress, TaskID, track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitIsADirectoryError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import AssetLoader, DataSetsLoader, LabelLoader, ResourceLoader
from cognite_toolkit._cdf_tk.utils import to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, yaml_safe_dump


class DataFinder(Generic[T_CogniteResource, T_CogniteResourceList]):
    def __init__(self, client: ToolkitClient, hierarchy: list[str] | None, data_set: list[str] | None) -> None:
        self.client = client
        self.loader = self._create_loader(client)
        self.hierarchy = hierarchy
        self.data_set = data_set

    @abstractmethod
    def _create_loader(self, client: ToolkitClient) -> ResourceLoader:
        """Create the appropriate loader for the finder."""
        raise NotImplementedError()

    def key(self, item: T_CogniteResource) -> str:
        """Return the key for grouping items. Default is empty string."""
        return ""

    def select_hierarchy_datasets(self) -> tuple[list[str], list[str]]:
        if self.hierarchy is None and self.data_set is None:
            return self.interactive_select()
        return self.hierarchy or [], self.data_set or []

    @abstractmethod
    def interactive_select(self) -> tuple[list[str], list[str]]:
        """Interactively select hierarchies and data sets."""
        raise NotImplementedError()

    @abstractmethod
    def aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        raise NotImplementedError()

    @abstractmethod
    def create_iterator(
        self, hierarchies: list[str], data_sets: list[str], limit: int | None
    ) -> Iterator[T_CogniteResourceList]:
        raise NotImplementedError()


class AssetFinder(DataFinder[Asset, AssetList]):
    def _create_loader(self, client: ToolkitClient) -> ResourceLoader:
        return AssetLoader.create_loader(client)

    def interactive_select(self) -> tuple[list[str], list[str]]:
        raise NotImplementedError()

    def aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
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

    def key(self, item: Asset) -> str:
        raise NotImplementedError()


class DumpData(ToolkitCommand):
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
        output_dir: Path,
        clean: bool,
        limit: int | None = None,
        format_: Literal["yaml", "csv", "parquet"] = "csv",
        verbose: bool = False,
    ) -> None:
        """Dumps data from CDF to a file

        Args:
            finder (DataFinder): The finder object to use for fetching data.
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

        hierarchies, data_sets = finder.select_hierarchy_datasets()
        if not hierarchies and not data_sets:
            raise ToolkitValueError("No hierarchy or data set provided")

        if not hierarchies and not data_sets:
            raise ToolkitValueError("No hierarchy or data set provided")

        total_items = finder.aggregate_count(hierarchies, data_sets)
        if limit:
            total_items = min(total_items, limit)

        loader = finder.loader
        with Progress() as progress:
            retrieved_time_series = progress.add_task(f"Retrieving {loader.display_name}", total=total_items)
            write_to_file = progress.add_task(f"Writing {loader.display_name} to file(s)", total=total_items)

            item_iterator = finder.create_iterator(hierarchies, data_sets, limit)
            item_iterator = self._log_retrieved(item_iterator, progress, retrieved_time_series)
            grouped_items = self._group_items(item_iterator, finder)
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

    def _write_data_sets(self, client: ToolkitClient, output_dir: Path, item_name: str) -> None:
        loader = DataSetsLoader.create_loader(client)
        data_sets = loader.retrieve(list(self._used_data_sets))
        if data_sets:
            to_dump = [loader.dump_resource(data_set) for data_set in data_sets]
            file_path = output_dir / DataSetsLoader.folder_name / f"{item_name}.{loader.kind}.yaml"
            self._write_to_yaml(to_dump, file_path)
            print(f"Dumped {len(data_sets):,} data sets to {file_path}")

    def _write_labels(self, client: ToolkitClient, output_dir: Path, item_name: str) -> None:
        loader = LabelLoader.create_loader(client)
        labels = loader.retrieve(list(self._used_labels))
        if labels:
            to_dump_dicts = [loader.dump_resource(item) for item in labels]

            file_path = output_dir / LabelLoader.folder_name / f"{item_name}.{loader.kind}.yaml"
            self._write_to_yaml(to_dump_dicts, file_path)

            print(f"Dumped {len(labels):,} labels to {file_path}")

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
    ) -> Iterator[tuple[str, T_CogniteResourceList]]:
        for item_list in items:
            for group, grouped_items in groupby(sorted(item_list, key=finder.key), key=finder.key):
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
        """Iterates over assets util the buffer reaches the filesize."""
        stored_items: dict[str, pd.DataFrame] = defaultdict(pd.DataFrame)
        for group, assets in item_iterator:
            stored_items[group] = pd.concat([stored_items[group], pd.DataFrame(assets)], ignore_index=True)
            if stored_items[group].memory_usage().sum() > self.buffer_size:
                yield group, stored_items.pop(group)
        for group, df in stored_items.items():
            if not df.empty:
                yield group, df
