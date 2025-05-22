import csv
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import lru_cache
from itertools import groupby
from pathlib import Path
from typing import Any, Callable, ClassVar, Literal, TypeAlias

import questionary
from cognite.client.data_classes import (
    Asset,
    AssetFilter,
    AssetList,
    DataSet,
    DataSetList,
    TimeSeriesFilter,
)
from rich.console import Console
from rich.progress import track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitIsADirectoryError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import AssetLoader, ResourceLoader, TimeSeriesLoader
from cognite_toolkit._cdf_tk.utils import to_directory_compatible
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, yaml_safe_dump

FileFormat: TypeAlias = Literal["csv", "parquet", "yaml"]


@dataclass
class SchemaColumn:
    name: str
    type: str


@dataclass
class Schema:
    display_name: str
    format_: FileFormat
    columns: list[SchemaColumn]


class DataFinder:
    supported_formats: ClassVar[frozenset[FileFormat]] = frozenset()

    def is_supported_format(self, format_: FileFormat) -> bool:
        return format_ in self.supported_formats

    @abstractmethod
    def create_iterators(
        self, format_: FileFormat, limit: int | None
    ) -> Iterator[tuple[Schema, int, Iterable, Callable]]:
        """Create an iterator for the specified format."""
        raise NotImplementedError("This method should be implemented in subclasses.")


class AssetCentricFinder(DataFinder, ABC):
    def __init__(self, client: ToolkitClient, user_hierarchy: list[str] | None, user_data_set: list[str] | None):
        self.client = client
        self.hierarchies, self.data_sets = self.select_hierarchy_datasets(user_hierarchy, user_data_set)
        self.loader = self._create_loader(client)
        self._used_labels: set[str] = set()
        self._used_data_sets: set[str] = set()

    @abstractmethod
    def _create_loader(self, client: ToolkitClient) -> ResourceLoader:
        """Create the appropriate loader for the finder."""
        raise NotImplementedError()

    @lru_cache
    def aggregate_count(self, hierarchies: tuple[str, ...], data_sets: tuple[str, ...]) -> int:
        return self._aggregate_count(list(hierarchies), list(data_sets))

    @abstractmethod
    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        raise NotImplementedError()

    def _to_write(self, items: Iterable) -> list[dict[str, Any]]:
        write_items: list[dict[str, Any]] = []
        for item in items:
            dumped = self.loader.dump_resource(item)
            if "metadata" in dumped:
                metadata = dumped.pop("metadata")
                for key, value in metadata.items():
                    dumped[f"metadata.{key}"] = value
            if isinstance(dumped.get("labels"), list):
                dumped["labels"] = [label["externalId"] for label in dumped["labels"]]
                self._used_labels.update(dumped["labels"])
            if "dataSetExternalId" in dumped:
                self._used_data_sets.add(dumped["dataSetExternalId"])
            write_items.append(dumped)
        return write_items

    def select_hierarchy_datasets(
        self, user_hierarchy: list[str] | None, user_data_set: list[str] | None
    ) -> tuple[list[str], list[str]]:
        if user_hierarchy or user_data_set:
            return user_hierarchy or [], user_data_set or []
        return self.interactive_select_hierarchy_datasets()

    def interactive_select_hierarchy_datasets(self) -> tuple[list[str], list[str]]:
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
                options = [asset for asset in self._get_available_hierarchies() if asset.external_id not in hierarchies]
                selected_hierarchy = self._select(what, options)
                if selected_hierarchy:
                    hierarchies.update(selected_hierarchy)
                else:
                    print("No hierarchy selected.")
            elif what == "Data Set":
                available_data_sets = [
                    data_set for data_set in self._get_available_data_sets() if data_set.external_id not in data_sets
                ]
                selected_data_set = self._select(what, available_data_sets)
                if selected_data_set:
                    data_sets.update(selected_data_set)
                else:
                    print("No data set selected.")
        return list(hierarchies), list(data_sets)

    def _select(self, what: str, options: list[Asset] | list[DataSet]) -> str | None:
        return questionary.checkbox(
            f"Select a {what} listed as 'name (external_id) [count]'",
            choices=[
                choice
                for choice, count in (
                    # MyPy does not seem to understand that item is Asset | DataSet
                    self._create_choice(item)  # type: ignore[arg-type]
                    for item in sorted(options, key=lambda x: x.name or x.external_id)
                )
                if count > 0
            ],
        ).ask()

    def _create_choice(self, item: Asset | DataSet) -> tuple[questionary.Choice, int]:
        """
        Choice with `title` including name and external_id if they differ.
        Adding `value` as external_id for the choice.
        `item_id` and `item` came in separate as item is DataSetWrite w/o `id`.
        """

        if isinstance(item, DataSet):
            if item.external_id is None:
                raise ValueError(f"Missing external ID for DataSet {item.id}")
            item_count = self._aggregate_count(tuple(), (item.external_id,))
        elif isinstance(item, Asset):
            if item.external_id is None:
                raise ValueError(f"Missing external ID for Asset {item.id}")
            item_count = self._aggregate_count((item.external_id,), tuple())
        else:
            raise TypeError(f"Unsupported item type: {type(item)}")

        return questionary.Choice(
            title=f"{item.name} ({item.external_id}) [{item_count:,}]"
            if item.name != item.external_id
            else f"({item.external_id}) [{item_count:,}]",
            value=item.external_id,
        ), item_count

    @lru_cache
    def _get_available_data_sets(self) -> DataSetList:
        return self.client.data_sets.list(limit=-1)

    @lru_cache
    def _get_available_hierarchies(self) -> AssetList:
        return self.client.assets.list(root=True, limit=-1)

    def _data_sets(self) -> tuple[Schema, int, Iterable, Callable]:
        data_sets = self.client.data_sets.list(external_ids=list(self._used_data_sets), limit=-1)
        return (
            Schema(display_name="data_sets", format_="yaml", columns=[]),
            len(data_sets),
            data_sets,
            lambda items: [(item.external_id, item.name, item.description) for item in items],
        )

    def _labels(self) -> tuple[Schema, int, Iterable, Callable]:
        labels = self.client.labels.list(external_ids=list(self._used_labels), limit=-1)
        return (
            Schema(display_name="labels", format_="yaml", columns=[]),
            len(labels),
            labels,
            lambda items: [(item.external_id, item.name, item.description) for item in items],
        )


class AssetFinder(AssetCentricFinder):
    supported_formats = frozenset({"csv", "parquet", "yaml"})

    def _create_loader(self, client: ToolkitClient) -> ResourceLoader:
        return AssetLoader.create_loader(client)

    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self.client.assets.aggregate_count(
            filter=AssetFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )

    def create_iterators(
        self, format_: FileFormat, limit: int | None
    ) -> Iterator[tuple[Schema, int, Iterable, Callable | None]]:
        total = self.aggregate_count(tuple(self.hierarchies), tuple(self.data_sets))
        columns = self._get_asset_columns()
        yield (
            Schema(display_name="assets", format_=format_, columns=columns),
            total,
            self.client.assets(
                chunk_size=1000,
                asset_subtree_external_ids=self.hierarchies or None,
                data_set_external_ids=self.data_sets or None,
                limit=limit,
            ),
            self._asset_processor,
        )
        if self._used_data_sets:
            yield self._data_sets()
        if self._used_labels:
            yield self._labels()

    def _asset_processor(self, assets: AssetList) -> list[tuple[str, list[dict[str, Any]]]]:
        grouped_assets: list[tuple[str, list[dict[str, object]]]] = []
        for group, asset_group in groupby(
            sorted([(self._group(asset), asset) for asset in assets], key=lambda x: x[0]), key=lambda x: x[0]
        ):
            grouped_assets.append((group, self._to_write(asset_group)))
        return grouped_assets

    def _group(self, item: Asset) -> str:
        if self.hierarchies and self.data_sets:
            asset_external_id = self.client.lookup.assets.external_id(item.root_id or 0)
            data_set_external_id = self.client.lookup.data_sets.external_id(item.data_set_id or 0)
            if asset_external_id and data_set_external_id:
                return f"{asset_external_id}.{data_set_external_id}"
            elif asset_external_id:
                return asset_external_id
            elif data_set_external_id:
                return data_set_external_id
            return ""
        elif self.hierarchies:
            return self.client.lookup.assets.external_id(item.root_id or 0) or ""
        elif self.data_sets:
            return self.client.lookup.data_sets.external_id(item.data_set_id or 0) or ""
        return ""


class TimeSeriesFinder(AssetCentricFinder):
    def _create_loader(self, client: ToolkitClient) -> TimeSeriesLoader:
        return TimeSeriesLoader.create_loader(client)

    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self.client.time_series.aggregate_count(
            filter=TimeSeriesFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )

    def create_iterators(
        self, format_: FileFormat, limit: int | None
    ) -> Iterator[tuple[Schema, int, Iterable, Callable | None]]:
        total = self.aggregate_count(tuple(self.hierarchies), tuple(self.data_sets))
        columns = self._get_time_series_columns(self.data_sets, self.hierarchies)
        yield (
            Schema(display_name="timeseries", format_=format_, columns=columns),
            total,
            self.client.time_series(
                chunk_size=1000,
                data_set_external_ids=self.data_sets or None,
                asset_subtree_external_ids=self.hierarchies or None,
                limit=limit,
            ),
            self._timeseries_preprocessor,
        )
        if self._used_data_sets:
            yield self._data_sets()
        if self._used_labels:
            yield self._labels()


class FileWriter:
    # 128 MB
    file_size = 128 * 1024 * 1024
    # Note the size in memory is not the same as the size on disk,
    # so the resulting file size will vary.
    encoding = "utf-8"
    newline = "\n"

    @abstractmethod
    def write_rows(self, group: str, rows: list[dict[str, Any]]) -> None:
        """Write rows to a file."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def load(cls, schema: Schema) -> "FileWriter":
        raise NotImplementedError()


class CSVWriter(FileWriter):
    def write_rows(self, group: str, rows: list[dict[str, Any]]) -> None:
        clean_name = to_directory_compatible(group) if group else "my"
        file_path = output_dir / loader.folder_name / f"{clean_name}.{loader.kind}.csv"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            if file_path.stat().st_size == 0:
                writer.writeheader()
            writer.writerows(rows)


class YAMLWriter(FileWriter):
    def write_rows(self, group: str, rows: list[dict[str, Any]]) -> None:
        clean_name = to_directory_compatible(group) if group else "my"
        file_path = output_dir / loader.folder_name / f"{clean_name}.{loader.kind}.yaml"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        if file_path.exists():
            with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                f.write("\n")
                f.write(yaml_safe_dump(item_list))
        else:
            with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                f.write(yaml_safe_dump(item_list))


class DumpDataCommand(ToolkitCommand):
    def dump_table(
        self,
        finder: DataFinder,
        output_dir: Path,
        clean: bool,
        limit: int | None = None,
        format_: FileFormat = "csv",
        verbose: bool = False,
    ) -> None:
        """Dumps data from CDF to files(s).

        Args:
            finder (DataFinder): The finder object to use for fetching data.
            output_dir (Path): The directory to write the output files to.
            clean (bool): Whether to clean the output directory before writing files.
            limit (int | None, optional): The maximum number of rows to write. Defaults to None.
            format_ (Literal["yaml", "csv", "parquet"], optional): The format of the output file. Defaults to "csv".
            verbose (bool, optional): Whether to print detailed progress information. Defaults to False.

        """
        if not finder.is_supported_format(format_):
            raise ToolkitValueError(f"Unsupported format {format_}. Supported formats are {finder.supported_formats}.")
        self.validate_directory(output_dir, clean)

        console = Console()
        for schema, total_items, resource_iterator, resource_processor in finder.create_iterators(format_, limit):
            file_writer = FileWriter.load(schema)
            for resources in track(resource_iterator, total=total_items, description=f"Dumping {schema.display_name}"):
                processed = resource_processor(resources)
                file_writer.write_rows(*processed)
            console.print(f"Dumped {total_items:,} rows to {output_dir}")

    @staticmethod
    def validate_directory(output_dir: Path, clean: bool) -> None:
        if output_dir.exists() and clean:
            safe_rmtree(output_dir)
        elif output_dir.exists():
            raise ToolkitFileExistsError(f"Output directory {output_dir!s} already exists. Use --clean to remove it.")
        elif output_dir.suffix:
            raise ToolkitIsADirectoryError(f"Output directory {output_dir!s} is not a directory.")
