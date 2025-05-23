import csv
import queue
import threading
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from functools import lru_cache
from itertools import groupby
from pathlib import Path
from types import MappingProxyType
from typing import Any, Callable, ClassVar, Generic, Literal, TypeAlias

import questionary
from cognite.client.data_classes import (
    Asset,
    AssetFilter,
    AssetList,
    DataSet,
    DataSetList,
    LabelDefinitionList,
    TimeSeries,
    TimeSeriesFilter,
)
from cognite.client.data_classes._base import T_CogniteResource
from rich.console import Console
from rich.progress import BarColumn, Progress, TaskID, TaskProgressColumn, TextColumn, TimeRemainingColumn, track

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitIsADirectoryError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.loaders import AssetLoader, DataSetsLoader, LabelLoader, ResourceLoader, TimeSeriesLoader
from cognite_toolkit._cdf_tk.utils import humanize_collection, to_directory_compatible
from cognite_toolkit._cdf_tk.utils.cdf import metadata_key_counts
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, yaml_safe_dump

FileFormat: TypeAlias = Literal["csv", "parquet", "yaml"]


@dataclass
class SchemaColumn:
    name: str
    type: str


@dataclass
class Schema:
    display_name: str
    folder_name: str
    kind: str
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


class AssetCentricFinder(DataFinder, ABC, Generic[T_CogniteResource]):
    def __init__(self, client: ToolkitClient, user_hierarchy: list[str] | None, user_data_set: list[str] | None):
        self.client = client
        self.hierarchies, self.data_sets = self.select_hierarchy_datasets(user_hierarchy, user_data_set)
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
    def _get_resource_columns(self) -> list[SchemaColumn]:
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
        yield (
            Schema(
                display_name=self.loader.display_name,
                format_=format_,
                columns=columns,
                folder_name=self.loader.folder_name,
                kind=self.loader.kind,
            ),
            total,
            self.create_resource_iterator(limit),
            self._resource_processor,
        )
        if self._used_data_sets:
            yield self._data_sets()
        if self._used_labels:
            yield self._labels()

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
            item_count = self.aggregate_count(tuple(), (item.external_id,))
        elif isinstance(item, Asset):
            if item.external_id is None:
                raise ValueError(f"Missing external ID for Asset {item.id}")
            item_count = self.aggregate_count((item.external_id,), tuple())
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
        data_sets = [d for d in self._get_available_data_sets() if d.external_id in self._used_data_sets]
        loader = DataSetsLoader.create_loader(self.client)

        def process_data_sets(items: DataSetList) -> list[tuple[str, list[dict[str, Any]]]]:
            return [("", [loader.dump_resource(item) for item in items])]

        return (
            # YAML format does not need columns.
            Schema(
                display_name=loader.display_name,
                format_="yaml",
                columns=[],
                folder_name=loader.folder_name,
                kind=loader.kind,
            ),
            len(data_sets),
            [data_sets],
            process_data_sets,
        )

    def _labels(self) -> tuple[Schema, int, Iterable, Callable]:
        labels = self.client.labels.retrieve(external_id=list(self._used_labels))
        loader = LabelLoader.create_loader(self.client)

        def process_labels(items: LabelDefinitionList) -> list[tuple[str, list[dict[str, Any]]]]:
            return [("", [loader.dump_resource(item) for item in items])]

        return (
            # YAML format does not need columns.
            Schema(
                display_name=loader.display_name,
                format_="yaml",
                columns=[],
                folder_name=loader.folder_name,
                kind=loader.kind,
            ),
            len(labels),
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
            chunk_size=1000,
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

    def _get_resource_columns(self) -> list[SchemaColumn]:
        columns = [
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="parentExternalId", type="string"),
            SchemaColumn(name="description", type="string"),
            SchemaColumn(name="dataSetExternalId", type="string"),
            SchemaColumn(name="source", type="string"),
            SchemaColumn(name="labels", type="array"),
            SchemaColumn(name="geoLocation", type="json"),
        ]
        data_set_ids = [
            dataset.id for dataset in self._get_available_data_sets() if dataset.external_id in self._data_set_set
        ] or None
        root_ids = [
            root.id for root in self._get_available_hierarchies() if root.external_id in self._hierarchy_set
        ] or None
        metadata_keys = metadata_key_counts(self.client, "assets", data_set_ids or None, root_ids or None)
        sorted_keys = sorted([key for item in metadata_keys for key in item.keys()])
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
            chunk_size=1000,
            asset_subtree_external_ids=self.hierarchies or None,
            data_set_external_ids=self.data_sets or None,
            limit=limit,
        )

    def _resource_processor(self, time_series: Iterable[TimeSeries]) -> list[tuple[str, list[dict[str, Any]]]]:
        return [("", self._to_write(time_series))]

    def _get_resource_columns(self) -> list[SchemaColumn]:
        columns = [
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="name", type="string"),
            SchemaColumn(name="isString", type="boolean"),
            SchemaColumn(name="unit", type="string"),
            SchemaColumn(name="unitExternalId", type="string"),
            SchemaColumn(name="assetExternalId", type="string"),
            SchemaColumn(name="isStep", type="boolean"),
            SchemaColumn(name="description", type="string"),
            SchemaColumn(name="dataSetExternalId", type="string"),
            SchemaColumn(name="securityCategories", type="array"),
        ]
        data_set_ids = [
            dataset.id for dataset in self._get_available_data_sets() if dataset.external_id in self._data_set_set
        ] or None
        root_ids = [
            root.id for root in self._get_available_hierarchies() if root.external_id in self._hierarchy_set
        ] or None
        metadata_keys = metadata_key_counts(self.client, "timeseries", data_set_ids or None, root_ids or None)
        sorted_keys = sorted([key for item in metadata_keys for key in item.keys()])
        columns.extend([SchemaColumn(name=f"metadata.{key}", type="string") for key in sorted_keys])
        return columns


class FileWriter:
    # 128 MB
    file_size = 128 * 1024 * 1024
    # Note the size in memory is not the same as the size on disk,
    # so the resulting file size will vary.
    encoding = "utf-8"
    newline = "\n"
    format: ClassVar[FileFormat]

    def __init__(self, schema: Schema, output_dir: Path) -> None:
        self.schema = schema
        self.output_dir = output_dir

    @abstractmethod
    def write_rows(self, rows: list[tuple[str, list[dict[str, Any]]]]) -> None:
        """Write rows to a file."""
        raise NotImplementedError("This method should be implemented in subclasses.")

    @classmethod
    def load(cls, schema: Schema, output_directory: Path) -> "FileWriter":
        write_cls = _FILEWRITER_CLASS_BY_FORMAT.get(schema.format_)
        if write_cls is None:
            raise ToolkitValueError(
                f"Unsupported format {schema.format_}. Supported formats are {humanize_collection(_FILEWRITER_CLASS_BY_FORMAT.keys())}."
            )

        return write_cls(schema, output_directory)


class CSVWriter(FileWriter):
    format = "csv"

    def write_rows(self, rows: list[tuple[str, list[dict[str, Any]]]]) -> None:
        for group, group_rows in rows:
            if not group_rows:
                continue
            clean_name = to_directory_compatible(group) if group else "my"
            file_path = self.output_dir / self.schema.folder_name / f"{clean_name}.{self.schema.kind}.csv"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                writer = csv.DictWriter(f, fieldnames=[col.name for col in self.schema.columns], extrasaction="ignore")
                if file_path.stat().st_size == 0:
                    writer.writeheader()
                writer.writerows(group_rows)


class YAMLWriter(FileWriter):
    format = "yaml"

    def write_rows(self, rows: list[tuple[str, list[dict[str, Any]]]]) -> None:
        for group, group_rows in rows:
            if not group_rows:
                continue
            clean_name = to_directory_compatible(group) if group else "my"
            file_path = self.output_dir / self.schema.folder_name / f"{clean_name}.{self.schema.kind}.yaml"
            file_path.parent.mkdir(parents=True, exist_ok=True)
            if file_path.exists():
                with file_path.open("a", encoding=self.encoding, newline=self.newline) as f:
                    f.write("\n")
                    f.write(yaml_safe_dump(group_rows))
            else:
                with file_path.open("w", encoding=self.encoding, newline=self.newline) as f:
                    f.write(yaml_safe_dump(group_rows))


class ProducerWorkerExecutor:
    def __init__(
        self,
        download_iterable: Iterable,
        process: Callable,
        write_to_file: Callable[[Any], None],
        total_items: int,
        max_queue_size: int,
    ) -> None:
        self._download_iterable = download_iterable
        self.download_complete = False
        self._write_to_file = write_to_file
        self._process = process
        self.console = Console()
        self.process_queue = queue.Queue(maxsize=max_queue_size)
        self.file_queue = queue.Queue(maxsize=max_queue_size)

        self.total_items = total_items
        self.error_occurred = False
        self.error_message = ""

    def run(self) -> None:
        with Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
            console=self.console,
        ) as progress:
            download_task = progress.add_task("Downloading", total=self.total_items)
            download_thread = threading.Thread(target=self._download_worker, args=(progress, download_task))
            process_thread: threading.Thread | None = None
            if self.process_queue:
                process_task = progress.add_task("Processing", total=self.total_items)
                process_thread = threading.Thread(target=self._process_worker, args=(progress, process_task))

            write_task = progress.add_task("Writing to file", total=self.total_items)
            write_thread = threading.Thread(target=self._write_worker, args=(progress, write_task))

            download_thread.start()
            if process_thread:
                process_thread.start()
            write_thread.start()

            # Wait for all threads to finish
            download_thread.join()
            if process_thread:
                process_thread.join()
            write_thread.join()

    def _download_worker(self, progress: Progress, download_task: TaskID) -> None:
        """Worker thread for downloading data."""
        iterable = iter(self._download_iterable)
        while True:
            try:
                items = next(iterable)
                while True:
                    try:
                        self.process_queue.put(items, timeout=0.5)
                        progress.update(download_task, advance=len(items))
                        break  # Exit the loop once the item is successfully added
                    except queue.Full:
                        # Retry until the queue has space
                        continue
            except StopIteration:
                self.download_complete = True
                break
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while downloading: {self.error_message}")
                break

    def _process_worker(self, progress: Progress, process_task: TaskID) -> None:
        """Worker thread for processing data."""
        if self._process is None or self.process_queue is None:
            return
        while not self.download_complete or not self.process_queue.empty():
            try:
                items = self.process_queue.get(timeout=0.5)
                processed_items = self._process(items)
                self.file_queue.put(processed_items)
                progress.update(process_task, advance=len(items))
            except queue.Empty:
                continue
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]ErrorError[/red] occurred while processing: {self.error_message}")
                break
            finally:
                self.process_queue.task_done()

    def _write_worker(self, progress: Progress, write_task: TaskID) -> None:
        """Worker thread for writing data to file."""
        # Simulate writing data to file
        while not self.download_complete or not self.file_queue.empty():
            try:
                items = self.file_queue.get(timeout=0.5)
                self._write_to_file(items)
                progress.update(write_task, advance=len(items))
            except queue.Empty:
                continue
            except Exception as e:
                self.error_occurred = True
                self.error_message = str(e)
                self.console.print(f"[red]Error[/red] occurred while writing: {self.error_message}")
                break
            finally:
                self.file_queue.task_done()


class DumpDataCommand(ToolkitCommand):
    def dump_table(
        self,
        finder: DataFinder,
        output_dir: Path,
        clean: bool,
        limit: int | None = None,
        format_: FileFormat = "csv",
        verbose: bool = False,
        parallel_threshold: int = 10_000,
        max_queue_size: int = 10,
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
            file_writer = FileWriter.load(schema, output_dir)
            if total_items > parallel_threshold:
                executor = ProducerWorkerExecutor(
                    resource_iterator,
                    file_writer.write_rows,
                    resource_processor,
                    total_items,
                    max_queue_size,
                )
                executor.run()
                if executor.error_occurred:
                    raise ToolkitValueError(executor.error_message)
            else:
                for resources in track(
                    resource_iterator, total=total_items, description=f"Dumping {schema.display_name}"
                ):
                    processed = resource_processor(resources)
                    file_writer.write_rows(processed)
            console.print(f"Dumped {total_items:,} rows to {output_dir}")

    @staticmethod
    def validate_directory(output_dir: Path, clean: bool) -> None:
        if output_dir.exists() and clean:
            safe_rmtree(output_dir)
        elif output_dir.exists():
            raise ToolkitFileExistsError(f"Output directory {output_dir!s} already exists. Use --clean to remove it.")
        elif output_dir.suffix:
            raise ToolkitIsADirectoryError(f"Output directory {output_dir!s} is not a directory.")


_FILEWRITER_CLASS_BY_FORMAT: MappingProxyType[str, type[FileWriter]] = MappingProxyType(
    {w.format: w for w in FileWriter.__subclasses__()}  # type: ignore[type-abstract]
)
