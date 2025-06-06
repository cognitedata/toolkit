from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from functools import lru_cache
from itertools import groupby
from pathlib import Path
from typing import Any, Callable, ClassVar, Generic, Literal

from cognite.client.data_classes import (
    Asset,
    AssetFilter,
    DataSetList,
    LabelDefinitionList,
    TimeSeries,
    TimeSeriesFilter,
)
from cognite.client.data_classes._base import T_CogniteResource
from rich.console import Console
from rich.progress import track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitIsADirectoryError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import AssetLoader, DataSetsLoader, LabelLoader, ResourceLoader, TimeSeriesLoader
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree
from cognite_toolkit._cdf_tk.utils.table_writers import (
    FileFormat,
    Schema,
    SchemaColumn,
    SchemaColumnList,
    TableFileWriter,
)


class DataFinder:
    supported_formats: ClassVar[frozenset[FileFormat]] = frozenset()
    # This is the standard maximum items that can be returns by most CDF endpoints.
    chunk_size: ClassVar[int] = 1000

    def is_supported_format(self, format_: FileFormat) -> bool:
        return format_ in self.supported_formats

    @abstractmethod
    def create_iterators(
        self, format_: FileFormat, limit: int | None
    ) -> Iterator[tuple[Schema, int, Iterable, Callable]]:
        """Create an iterator for the specified format."""
        raise NotImplementedError("This method should be implemented in subclasses.")


class AssetCentricFinder(DataFinder, ABC, Generic[T_CogniteResource]):
    def __init__(self, client: ToolkitClient, hierarchies: list[str], data_sets: list[str]):
        self.client = client
        self.hierarchies = hierarchies
        self.data_sets = data_sets
        self.loader = self._create_loader(client)
        self._hierarchy_set = set(self.hierarchies)
        self._data_set_set = set(self.data_sets)
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

    @abstractmethod
    def _get_resource_columns(self) -> SchemaColumnList:
        """Get the columns for the schema."""
        raise NotImplementedError()

    @abstractmethod
    def create_resource_iterator(self, limit: int | None) -> Iterable:
        raise NotImplementedError()

    @abstractmethod
    def _resource_processor(self, items: Iterable[T_CogniteResource]) -> list[tuple[str, list[dict[str, Any]]]]:
        """Process the resources and return them in a format suitable for writing."""
        raise NotImplementedError()

    def _to_write(self, items: Iterable[T_CogniteResource]) -> list[dict[str, Any]]:
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

    def create_iterators(
        self, format_: FileFormat, limit: int | None
    ) -> Iterator[tuple[Schema, int, Iterable, Callable]]:
        total = self.aggregate_count(tuple(self.hierarchies), tuple(self.data_sets))
        columns = self._get_resource_columns()

        iteration_count = total // self.chunk_size + (1 if total % self.chunk_size > 0 else 0)
        if iteration_count == 0:
            return

        yield (
            Schema(
                display_name=self.loader.display_name,
                format_=format_,
                columns=columns,
                folder_name=self.loader.folder_name,
                kind=self.loader.kind,
            ),
            iteration_count,
            self.create_resource_iterator(limit),
            self._resource_processor,
        )
        if self._used_data_sets:
            yield self._data_sets()
        if self._used_labels:
            yield self._labels()

    def _data_sets(self) -> tuple[Schema, int, Iterable, Callable]:
        data_sets = self.client.data_sets.retrieve_multiple(
            external_ids=list(self._used_data_sets), ignore_unknown_ids=True
        )
        loader = DataSetsLoader.create_loader(self.client)

        def process_data_sets(items: DataSetList) -> list[tuple[str, list[dict[str, Any]]]]:
            # All data sets are written to a single group, thus the empty string as the group key.
            # (Group keys are for example used in CSV files to create separate files for each
            # data set an asset belongs to.)
            return [("", [loader.dump_resource(item) for item in items])]

        return (
            # YAML format does not need columns.
            Schema(
                display_name=loader.display_name,
                format_="yaml",
                columns=SchemaColumnList(),
                folder_name=loader.folder_name,
                kind=loader.kind,
            ),
            1,
            [data_sets],
            process_data_sets,
        )

    def _labels(self) -> tuple[Schema, int, Iterable, Callable]:
        labels = self.client.labels.retrieve(external_id=list(self._used_labels))
        loader = LabelLoader.create_loader(self.client)

        def process_labels(items: LabelDefinitionList) -> list[tuple[str, list[dict[str, Any]]]]:
            # All labels are written to a single group, thus the empty string as the group key.
            # (Group keys are for example used in CSV files to create separate files for each
            # label an asset belongs to.)
            return [("", [loader.dump_resource(item) for item in items])]

        return (
            # YAML format does not need columns.
            Schema(
                display_name=loader.display_name,
                format_="yaml",
                columns=SchemaColumnList(),
                folder_name=loader.folder_name,
                kind=loader.kind,
            ),
            1,
            [labels],
            process_labels,
        )


class AssetFinder(AssetCentricFinder[Asset]):
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

    def create_resource_iterator(self, limit: int | None) -> Iterator:
        return self.client.assets(
            chunk_size=self.chunk_size,
            asset_subtree_external_ids=self.hierarchies or None,
            data_set_external_ids=self.data_sets or None,
            limit=limit,
        )

    def _resource_processor(self, assets: Iterable[Asset]) -> list[tuple[str, list[dict[str, Any]]]]:
        grouped_assets: list[tuple[str, list[dict[str, object]]]] = []
        for group, asset_group in groupby(
            sorted([(self._group(asset), asset) for asset in assets], key=lambda x: x[0]), key=lambda x: x[0]
        ):
            grouped_assets.append((group, self._to_write([asset for _, asset in asset_group])))
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

    def _get_resource_columns(self) -> SchemaColumnList:
        columns = SchemaColumnList(
            [
                SchemaColumn(name="externalId", type="string"),
                SchemaColumn(name="name", type="string"),
                SchemaColumn(name="parentExternalId", type="string"),
                SchemaColumn(name="description", type="string"),
                SchemaColumn(name="dataSetExternalId", type="string"),
                SchemaColumn(name="source", type="string"),
                SchemaColumn(name="labels", type="string", is_array=True),
                SchemaColumn(name="geoLocation", type="json"),
            ]
        )
        data_set_ids = self.client.lookup.data_sets.id(self.data_sets) if self.data_sets else []
        root_ids = self.client.lookup.assets.id(self.hierarchies) if self.hierarchies else []
        metadata_keys = metadata_key_counts(self.client, "assets", data_set_ids or None, root_ids or None)
        sorted_keys = sorted([key for key, count in metadata_keys if count > 0])
        columns.extend([SchemaColumn(name=f"metadata.{key}", type="string") for key in sorted_keys])
        return columns


class TimeSeriesFinder(AssetCentricFinder[TimeSeries]):
    supported_formats = frozenset({"csv", "parquet", "yaml"})

    def _create_loader(self, client: ToolkitClient) -> TimeSeriesLoader:
        return TimeSeriesLoader.create_loader(client)

    def _aggregate_count(self, hierarchies: list[str], data_sets: list[str]) -> int:
        return self.client.time_series.aggregate_count(
            filter=TimeSeriesFilter(
                data_set_ids=[{"externalId": item} for item in data_sets] or None,
                asset_subtree_ids=[{"externalId": item} for item in hierarchies] or None,
            )
        )

    def create_resource_iterator(self, limit: int | None) -> Iterator:
        return self.client.time_series(
            chunk_size=self.chunk_size,
            asset_subtree_external_ids=self.hierarchies or None,
            data_set_external_ids=self.data_sets or None,
            limit=limit,
        )

    def _resource_processor(self, time_series: Iterable[TimeSeries]) -> list[tuple[str, list[dict[str, Any]]]]:
        return [("", self._to_write(time_series))]

    def _get_resource_columns(self) -> SchemaColumnList:
        columns = SchemaColumnList(
            [
                SchemaColumn(name="externalId", type="string"),
                SchemaColumn(name="name", type="string"),
                SchemaColumn(name="isString", type="boolean"),
                SchemaColumn(name="unit", type="string"),
                SchemaColumn(name="unitExternalId", type="string"),
                SchemaColumn(name="assetExternalId", type="string"),
                SchemaColumn(name="isStep", type="boolean"),
                SchemaColumn(name="description", type="string"),
                SchemaColumn(name="dataSetExternalId", type="string"),
                SchemaColumn(name="securityCategories", type="string", is_array=True),
            ]
        )
        data_set_ids = self.client.lookup.data_sets.id(self.data_sets) if self.data_sets else []
        root_ids = self.client.lookup.assets.id(self.hierarchies) if self.hierarchies else []
        metadata_keys = metadata_key_counts(self.client, "timeseries", data_set_ids or None, root_ids or None)
        sorted_keys = sorted([key for key, count in metadata_keys if count > 0])
        columns.extend([SchemaColumn(name=f"metadata.{key}", type="string") for key in sorted_keys])
        return columns


class DumpDataCommand(ToolkitCommand):
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
        if not finder.is_supported_format(format_):
            raise ToolkitValueError(f"Unsupported format {format_}. Supported formats are {finder.supported_formats}.")
        self.validate_directory(output_dir, clean)

        console = Console()
        for schema, iteration_count, resource_iterator, resource_processor in finder.create_iterators(format_, limit):
            writer_cls = TableFileWriter.get_write_cls(schema.format_)
            row_counts = 0
            with writer_cls(schema, output_dir) as writer:
                for resources in track(
                    resource_iterator, total=iteration_count, description=f"Dumping {schema.display_name}"
                ):
                    row_counts += len(resources)
                    processed = resource_processor(resources)
                    writer.write_rows(processed)
            console.print(f"Dumped {row_counts:,} rows to {output_dir}")

    @staticmethod
    def validate_directory(output_dir: Path, clean: bool) -> None:
        if output_dir.exists() and clean:
            safe_rmtree(output_dir)
        elif output_dir.exists():
            raise ToolkitFileExistsError(f"Output directory {output_dir!s} already exists. Use --clean to remove it.")
        elif output_dir.suffix:
            raise ToolkitIsADirectoryError(f"Output directory {output_dir!s} is not a directory.")
