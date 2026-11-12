from collections import Counter
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Literal, TypeAlias

import questionary
from pydantic import BaseModel, Field
from questionary import Choice
from rich.console import Console
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsFailedRequest,
    ItemsFailedResponse,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ViewCorePropertyResponse,
    ViewId,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeployOptions, DeployV2Command
from cognite_toolkit._cdf_tk.constants import DATA_MANIFEST_SUFFIX, DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.data_classes._tracking_info import DataTracking
from cognite_toolkit._cdf_tk.dataio import (
    CanvasIO,
    ChartIO,
    CogniteFileContentIO,
    FileContentIO,
    FileMetadataContentIO,
    Page,
    T_DataRequest,
    T_DataResponse,
    T_Selector,
    TableDataIO,
    UploadableDataIO,
    get_upload_io,
)
from cognite_toolkit._cdf_tk.dataio._base import TableUploadableDataIO
from cognite_toolkit._cdf_tk.dataio.logger import (
    DataLogger,
    FileWithAggregationLogger,
    LogEntryV2,
    Severity,
    display_item_results,
)
from cognite_toolkit._cdf_tk.dataio.selectors import SelectedView, Selector, load_selector, load_selector_from_dict
from cognite_toolkit._cdf_tk.dataio.selectors._instances import InstanceSpaceSelector, InstanceViewSelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitRepeatedUploadFailureError, ToolkitValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import ViewIO
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, MediumSeverityWarning, ToolkitWarning
from cognite_toolkit._cdf_tk.tracker import Tracker
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import create_logfile_stem, safe_write, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import (
    CSVReader,
    MultiFileReader,
    NDJsonWriter,
    ParquetReader,
    SQLiteReader,
    Uncompressed,
)
from cognite_toolkit._cdf_tk.utils.interactive_select import DataModelingSelect, ViewSelectFilter
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import ToolkitCommand

# SQLite databases are not included, as they store the selector with each table instead of in a manifest.
INSTANCE_FILE_READER_BY_FORMAT: Mapping[str, type[CSVReader | ParquetReader]] = {
    reader.FORMAT: reader for reader in (CSVReader, ParquetReader)
}
_NODE_COLUMNS = ("space", "externalId", "existingVersion", "type.space", "type.externalId")
_EDGE_ENDPOINT_COLUMNS = ("startNode.space", "startNode.externalId", "endNode.space", "endNode.externalId")
INSTANCE_COLUMNS_BY_TYPE: Mapping[Literal["node", "edge"], tuple[str, ...]] = {
    "node": _NODE_COLUMNS,
    "edge": _NODE_COLUMNS + _EDGE_ENDPOINT_COLUMNS,
}
REQUIRED_INSTANCE_COLUMNS_BY_TYPE: Mapping[Literal["node", "edge"], tuple[str, ...]] = {
    "node": ("space", "externalId"),
    "edge": ("space", "externalId", "type.space", "type.externalId", *_EDGE_ENDPOINT_COLUMNS),
}


class InstanceColumnMatch(BaseModel):
    """How the columns of a table file map to an instance upload through a view."""

    instance_columns: list[str] = Field(default_factory=list)
    property_type_by_column: dict[str, str] = Field(default_factory=dict)
    unknown_columns: list[str] = Field(default_factory=list)


@dataclass(frozen=True)
class SQLiteTableSource:
    database: Path
    table: str


DataSource: TypeAlias = list[Path] | SQLiteTableSource


class UploadCommand(ToolkitCommand):
    """Command for uploading data to CDF from files in a specified directory.

    This command reads files matching a specific pattern in the input directory,
    processes them using a StorageIO instance, and uploads the data to CDF.

    Attributes:
        _MAX_QUEUE_SIZE (int): The maximum size of the queue for processing items.
            Set to 80 to balance memory usage and processing speed.
    """

    _MAX_QUEUE_SIZE = 80
    _MAX_VERBOSE_PRINTED_FAILED_IDS = 10

    def upload(
        self,
        input_dir: Path,
        client: ToolkitClient,
        deploy_resources: bool,
        dry_run: bool,
        verbose: bool,
        skip_strict_mode: bool = False,
        overwrite: bool = False,
        kind: str | None = None,
    ) -> None:
        """Uploads data from files in the specified input directory to CDF.

        Args:
            input_dir: The directory containing the files to upload. It is expected to
                have the structure defined below.
            client: An instance of ToolkitClient to interact with CDF.
            deploy_resources: If True, deploys resources from the 'resources' subdirectory.
            dry_run: If True, performs a dry run without actually uploading the data
                (or deploying resources).
            verbose: If True, prints detailed information about the upload process.
            skip_strict_mode: If True, skips strict mode when uploading Charts with monitoring jobs and/or
                scheduled calculations.
            overwrite: If the data type supports it, overwrite in CDF.
            kind: Optional; if provided, only data files of this kind will be processed.

        The expected structure of the input directory is as follows:
        ```
        input_dir/
        ├── resources/                # Optional, only if deploy_resources is True
        │   ├── raw/
        │   │   ├── table1.Table.yaml
        │   │   └── table2.Table.yaml
        │   └── ...
        ├── datafile1.kind.ndjson # Data file of a specific kind
        ├── datafile1.Manifest.yaml       # Manifest for datafile1
        ├── datafile2.kind2.ndjson # Another data file of the same or different kind
        ├── datafile2.Manifest.yaml       # Manifest file for datafile2
        ├── kind.sqlite                   # SQLite database with one table per selector (alpha)
        └── ...
        """
        console = client.console
        data_files_by_selector = self._find_data_files(input_dir)

        self._deploy_resource_folder(
            input_dir / DATA_RESOURCE_DIR, client, deploy_resources, client.config.project, dry_run, verbose
        )

        data_files_by_selector = self._topological_sort_if_instance_selector(data_files_by_selector, client)

        total_file_count = sum(
            len(source) if isinstance(source, list) else 1 for source in data_files_by_selector.values()
        )
        if verbose:
            input_dir_display = self._path_as_display_name(input_dir)
            console.print(f"Found {total_file_count} files to upload in {input_dir_display.as_posix()!r}.")
        self.upload_data(
            data_files_by_selector,
            input_dir,
            client,
            dry_run,
            console,
            verbose,
            self.tracker,
            skip_strict_mode,
            overwrite,
        )

    def upload_instances(
        self,
        data_file: Path,
        client: ToolkitClient,
        verbose: bool,
        validate_project: Callable[[], None] | None = None,
    ) -> None:
        """Guides the user through creating a manifest for a CSV or Parquet file and uploading it as instances.

        Args:
            data_file: The CSV or Parquet file with one instance per row.
            client: An instance of ToolkitClient to interact with CDF.
            verbose: If True, prints detailed information about the upload process.
            validate_project: Called after the user has chosen to upload or dry run, to confirm the CDF project.
        """
        selector = self.create_instance_manifest(data_file, client)
        if selector is None:
            return
        action = questionary.select(
            f"Do you want to upload {data_file.name!r} to CDF now?",
            choices=[
                Choice(title="Yes, upload now", value="upload"),
                Choice(title="Dry run, show what would be uploaded", value="dry-run"),
                Choice(title="No, only create the manifest", value="skip"),
            ],
        ).unsafe_ask()
        if action == "skip":
            client.console.print(
                f"Run 'cdf data upload dir {self._path_as_display_name(data_file.parent).as_posix()}' "
                "to upload the file later."
            )
            return
        if validate_project is not None:
            validate_project()
        data_files_by_selector: dict[Selector, DataSource] = {selector: [data_file]}
        self.upload_data(
            data_files_by_selector,
            data_file.parent,
            client,
            dry_run=action == "dry-run",
            console=client.console,
            verbose=verbose,
            tracker=self.tracker,
        )

    def create_instance_manifest(self, data_file: Path, client: ToolkitClient) -> InstanceViewSelector | None:
        """Interactively selects the view to upload the data file to and writes the manifest next to the file.

        Returns:
            The selector written to the manifest, or None if the user chose not to create the manifest.
        """
        if data_file.suffix == SQLiteReader.FORMAT:
            raise ToolkitValueError(
                f"Cannot create a manifest for {data_file.name!r}. SQLite databases store the selector with each "
                f"table, upload them with 'cdf data upload dir'."
            )
        reader_cls = INSTANCE_FILE_READER_BY_FORMAT.get(data_file.suffix)
        if reader_cls is None:
            raise ToolkitValueError(
                f"Cannot create a manifest for {data_file.name!r}. "
                f"Supported formats: {humanize_collection(INSTANCE_FILE_READER_BY_FORMAT.keys())}."
            )
        columns = [column.name for column in reader_cls.sniff_schema(data_file, sniff_rows=1)]

        dm_select = DataModelingSelect(client, "upload instances", console=client.console)
        view = dm_select.select_view(
            message="Select the view to upload the instances to (type to search):",
            filter=ViewSelectFilter(strategy="all", include_global=True),
        )
        instance_type = dm_select.select_instance_type(
            view.used_for, message=f"What type of instances does {data_file.name!r} contain?"
        )
        match = self.match_instance_columns(columns, view, instance_type)
        client.console.print(self._create_column_table(match, data_file, view))

        if match.unknown_columns:
            self.warn(
                MediumSeverityWarning(
                    f"The view {view.space}:{view.external_id}(version={view.version}) does not have the "
                    f"properties {humanize_collection(match.unknown_columns)}. The upload will fail unless you "
                    "rename or remove these columns."
                ),
                console=client.console,
            )
            if not questionary.confirm("Do you want to create the manifest anyway?", default=False).unsafe_ask():
                return None

        manifest_file = data_file.with_name(f"{data_file.stem}{DATA_MANIFEST_SUFFIX}")
        manifest_display = self._path_as_display_name(manifest_file).as_posix()
        if (
            manifest_file.exists()
            and not questionary.confirm(
                f"The manifest {manifest_display!r} already exists. Do you want to overwrite it?", default=False
            ).unsafe_ask()
        ):
            return None

        selector = InstanceViewSelector(
            view=SelectedView(space=view.space, external_id=view.external_id, version=view.version),
            instance_type=instance_type,
        )
        manifest = selector.model_dump(mode="json", by_alias=True, exclude_none=True)
        safe_write(manifest_file, yaml_safe_dump(manifest), encoding="utf-8")
        client.console.print(f"Created manifest {manifest_display!r}.")

        other_files = [file for file in selector.find_data_files(data_file.parent, manifest_file) if file != data_file]
        if other_files:
            self.warn(
                MediumSeverityWarning(
                    f"'cdf data upload dir' will also upload {humanize_collection([f.name for f in other_files])} "
                    f"with this manifest, as their names start with {data_file.stem!r}."
                ),
                console=client.console,
            )
        return selector

    @staticmethod
    def match_instance_columns(
        columns: Sequence[str], view: ViewResponse, instance_type: Literal["node", "edge"]
    ) -> InstanceColumnMatch:
        """Matches the columns of a table file to the instance fields and the properties of the view.

        Raises:
            ToolkitValueError: If a column required to identify the instances is missing.
        """
        missing = [column for column in REQUIRED_INSTANCE_COLUMNS_BY_TYPE[instance_type] if column not in columns]
        if missing:
            raise ToolkitValueError(
                f"The file is missing the column(s) {humanize_collection(missing)}, which are required to upload "
                f"{instance_type}s."
            )
        instance_columns = INSTANCE_COLUMNS_BY_TYPE[instance_type]
        properties = {
            prop_id: prop for prop_id, prop in view.properties.items() if isinstance(prop, ViewCorePropertyResponse)
        }
        match = InstanceColumnMatch()
        for column in columns:
            if column in instance_columns:
                match.instance_columns.append(column)
            elif column in properties:
                match.property_type_by_column[column] = properties[column].type.type
            else:
                match.unknown_columns.append(column)
        return match

    @staticmethod
    def _create_column_table(match: InstanceColumnMatch, data_file: Path, view: ViewResponse) -> Table:
        table = Table(title=f"Columns in {data_file.name!r} matched to view {view.space}:{view.external_id}")
        table.add_column("Column")
        table.add_column("Used as")
        for column in match.instance_columns:
            table.add_row(column, "Instance field")
        for column, property_type in match.property_type_by_column.items():
            table.add_row(column, f"Property ({property_type})")
        for column in match.unknown_columns:
            table.add_row(column, "[red]Not a property in the view[/red]")
        return table

    def _topological_sort_if_instance_selector(
        self, data_files_by_selector: Mapping[Selector, DataSource], client: ToolkitClient
    ) -> Mapping[Selector, DataSource]:
        """Topologically sorts InstanceSelectors (if they are present) to determine the order of upload based on container dependencies from the views.

        Args:
            data_files_by_selector: A dictionary mapping selectors to their data files.
            client: The cognite client to use for the upload.

        Returns:
            A dictionary mapping selectors to their data files with necessary preprocessing.
        """
        counts = Counter(type(selector) for selector in data_files_by_selector.keys())
        if (counts[InstanceSpaceSelector] + counts[InstanceViewSelector]) <= 1:
            return data_files_by_selector

        selector_by_view_id: dict[ViewId, Selector] = {}
        for selector in data_files_by_selector:
            if isinstance(selector, InstanceSpaceSelector | InstanceViewSelector) and selector.view is not None:
                view_ref = selector.view.as_id()
                if not isinstance(view_ref, ViewId):
                    raise RuntimeError(
                        f"View {view_ref} does not have a version, which is required for topological sorting."
                    )
                selector_by_view_id[view_ref] = selector

        view_dependencies, cyclic_views = ViewIO.create_io(client).topological_sort_container_constraints(
            list(selector_by_view_id.keys())
        )
        if cyclic_views:
            MediumSeverityWarning(
                f"Cyclic dependencies detected between views: {', '.join(str(view) for view in cyclic_views)}. "
                f"If their constraints are not already satisfied by instances in existing containers / views "
                f"in CDF, the upload may fail for these views."
            ).print_warning(console=client.console)
            view_dependencies = view_dependencies + cyclic_views

        prepared_selectors: dict[Selector, DataSource] = {}

        # Reorder selectors according to the dependency-sorted view list
        for view_id in view_dependencies:
            selector = selector_by_view_id[view_id]
            prepared_selectors[selector] = data_files_by_selector[selector]

        # Preserve selectors that aren't affected by view dependencies
        # (e.g., raw tables, time series, non-view instance data)
        for selector in data_files_by_selector.keys():
            if selector not in prepared_selectors:
                prepared_selectors[selector] = data_files_by_selector[selector]

        return prepared_selectors

    def _find_data_files(
        self,
        input_dir: Path,
    ) -> dict[Selector, DataSource]:
        """Finds data files and their corresponding metadata files, and SQLite databases, in the input directory."""
        data_files_by_metadata: dict[Selector, DataSource] = {}
        for manifest_file in input_dir.glob(f"*{DATA_MANIFEST_SUFFIX}"):
            selector_or_warning = load_selector(manifest_file)
            if isinstance(selector_or_warning, ToolkitWarning):
                self.warn(selector_or_warning)
                continue
            selector: Selector = selector_or_warning
            data_files = selector.find_data_files(input_dir, manifest_file)
            if not data_files:
                self.warn(
                    MediumSeverityWarning(
                        f"Metadata file {manifest_file.as_posix()!r} has no corresponding data files, skipping.",
                    )
                )
                continue
            data_files_by_metadata[selector] = data_files
        data_files_by_metadata.update(self._find_sqlite_tables(input_dir))
        return data_files_by_metadata

    def _find_sqlite_tables(self, input_dir: Path) -> dict[Selector, DataSource]:
        """Finds the tables in SQLite databases in the input directory. The selector is stored with each table."""
        tables_by_selector: dict[Selector, DataSource] = {}
        for database in sorted(input_dir.glob(f"*{SQLiteReader.FORMAT}")):
            if not Flags.EXTEND_DOWNLOAD.is_enabled():
                self.warn(
                    MediumSeverityWarning(
                        f"Skipping {database.as_posix()!r}. Uploading SQLite databases is currently behind an alpha "
                        "feature flag. Enable the flag `extend_download` under [alpha_flags] in cdf.toml."
                    )
                )
                continue
            try:
                tables = SQLiteReader.list_tables(database)
            except ToolkitValueError as e:
                self.warn(MediumSeverityWarning(f"Skipping {database.as_posix()!r}. {e}"))
                continue
            for table in tables:
                if table.metadata is None:
                    self.warn(
                        MediumSeverityWarning(
                            f"Table {table.name!r} in {database.as_posix()!r} has no selector, skipping."
                        )
                    )
                    continue
                selector_or_warning = load_selector_from_dict(table.metadata, database)
                if isinstance(selector_or_warning, ToolkitWarning):
                    self.warn(selector_or_warning)
                    continue
                tables_by_selector[selector_or_warning] = SQLiteTableSource(database, table.name)
        return tables_by_selector

    def _deploy_resource_folder(
        self,
        resource_dir: Path,
        client: ToolkitClient,
        deploy_resources: bool,
        cdf_project: str,
        dry_run: bool,
        verbose: bool,
    ) -> None:
        """Deploy resources from the specified resource directory if it exists and deployment is enabled."""
        if deploy_resources and resource_dir.exists():
            deploy_cmd = DeployV2Command(self.print_warning, silent=self.silent)
            deploy_cmd.tracker = self.tracker
            deploy_cmd.deploy(
                user_build_dir=resource_dir,
                client=client,
                options=DeployOptions(operation="deploy", cdf_project=cdf_project, dry_run=dry_run, verbose=verbose),
            )
            self.warning_list.extend(deploy_cmd.warning_list)
        elif deploy_resources:
            self.warn(
                MediumSeverityWarning(
                    f"Resource directory {resource_dir.as_posix()!r} does not exist, skipping resource deployment."
                )
            )

    @classmethod
    def upload_data(
        cls,
        data_files_by_selector: Mapping[Selector, DataSource],
        input_dir: Path,
        client: ToolkitClient,
        dry_run: bool,
        console: Console,
        verbose: bool,
        tracker: Tracker,
        skip_strict_mode: bool = False,
        overwrite: bool = False,
    ) -> None:
        action = "Would upload" if dry_run else "Uploading"

        input_dir.mkdir(parents=True, exist_ok=True)
        log_filestem = create_logfile_stem(input_dir, "upload")
        with (
            NDJsonWriter(
                input_dir, kind="UploadIssues", default_filestem=log_filestem, compression=Uncompressed
            ) as log_file,
            FileWithAggregationLogger(log_file) as logger,
            HTTPClient(config=client.config) as upload_client,
        ):
            for selector, source in data_files_by_selector.items():
                first_file = source.database if isinstance(source, SQLiteTableSource) else source[0]
                io = cls._create_selected_io(selector, first_file, client, skip_strict_mode, overwrite)
                if io is None:
                    continue
                io.logger = logger
                logger.reset()
                # Create reader first to determine if input is table format
                if isinstance(source, SQLiteTableSource):
                    # SQLite databases store the schema with the table.
                    reader = MultiFileReader([source.database], table=source.table)
                else:
                    reader = MultiFileReader(source)
                # FileContentIO supports uploading any file format.
                if reader.is_table and not isinstance(io, TableUploadableDataIO | FileContentIO):
                    raise ToolkitValueError(
                        f"{selector.type}.{selector.kind} does not support {reader.format!r} files."
                    )
                # Only fetch schema for table formats (e.g., CSV, Parquet), not for JSON formats
                if reader.is_table and isinstance(io, TableDataIO) and isinstance(source, list):
                    schema = io.get_schema(selector)
                    reader = MultiFileReader(source, schema=schema)

                if isinstance(io, CanvasIO):
                    for warning in io.check_view_references(reader):
                        warning.print_warning(console=console)

                item_count = io.count_items(reader, selector)

                def read_chunks_with_registered_pages() -> Iterator[Page[dict[str, JsonVal]]]:
                    for page in io.read_chunks(reader, selector):
                        yield io.emit_registered_page(page)

                executor = ProducerWorkerExecutor[Page[dict[str, JsonVal]], Page](
                    download_iterable=read_chunks_with_registered_pages(),
                    process=partial(io.rows_to_data, selector=selector)
                    if reader.is_table and isinstance(io, TableUploadableDataIO)
                    else io.json_chunk_to_data,
                    write=partial(
                        cls._upload_items,
                        upload_client=upload_client,
                        io=io,
                        dry_run=dry_run,
                        selector=selector,
                        console=console,
                        verbose=verbose,
                        logger=logger,
                        get_log_file=lambda: log_file.latest_file,
                    ),
                    total_item_count=item_count,
                    max_queue_size=cls._MAX_QUEUE_SIZE,
                    download_description="Reading files",
                    process_description="Processing",
                    write_description=f"{action} {selector.display_name}",
                    console=console,
                )
                executor.run()

                if isinstance(executor.error_exception, ToolkitRepeatedUploadFailureError):
                    logger.apply_to_all_unprocessed(
                        label="Early termination of upload process", severity=Severity.skipped
                    )
                    logger.force_write()

                items_results = logger.finalize(dry_run)
                display_item_results(items_results, title=f"Finished upload {selector.display_name}", console=console)
                tracker.track(DataTracking.from_item_results("UploadResult", selector.kind, items_results), client)
                if isinstance(executor.error_exception, ToolkitRepeatedUploadFailureError):
                    console.print(
                        f"[yellow]Skipping uploading further items for {selector.display_name} after "
                        f"encountering repeated failures. Check the log files in {input_dir}.[/yellow]"
                    )
                else:
                    executor.raise_on_error()

    @staticmethod
    def _path_as_display_name(input_path: Path, cwd: Path = Path.cwd()) -> Path:
        display_name = input_path
        if input_path.is_relative_to(cwd):
            display_name = input_path.relative_to(cwd)
        return display_name

    @classmethod
    def _create_selected_io(
        cls, selector: Selector, data_file: Path, client: ToolkitClient, skip_strict_mode: bool, overwrite: bool
    ) -> UploadableDataIO | None:
        try:
            io_cls = get_upload_io(selector)
        except ValueError as e:
            HighSeverityWarning(f"Could not find StorageIO for selector {selector}: {e}").print_warning(
                console=client.console
            )
            return None
        if issubclass(io_cls, ChartIO):
            return ChartIO(client, skip_strict_mode=skip_strict_mode)
        elif issubclass(io_cls, FileMetadataContentIO | CogniteFileContentIO):
            return io_cls(client, config_directory=data_file.parent, overwrite=overwrite)
        else:
            return io_cls(client)

    @classmethod
    def _upload_items(
        cls,
        data_chunk: Page[T_DataRequest],
        upload_client: HTTPClient,
        io: UploadableDataIO[T_Selector, T_DataResponse, T_DataRequest],
        selector: T_Selector,
        dry_run: bool,
        console: Console,
        verbose: bool,
        logger: DataLogger,
        get_log_file: Callable[[], Path | None],
    ) -> None:
        if dry_run:
            return
        results = io.upload_items(data_chunk, upload_client, selector)
        all_failed = True
        for message in results:
            if isinstance(message, ItemsSuccessResponse):
                all_failed = False
            elif isinstance(message, ItemsFailedRequest | ItemsFailedResponse):
                label = "Failed request"
                error_description = message.error_message
                if isinstance(message, ItemsFailedResponse):
                    label = f"HTTP {message.status_code} code"
                elif isinstance(message, ItemsFailedRequest):
                    label = "Failed request"
                for id_ in message.ids:
                    logger.log(LogEntryV2(id=id_, label=label, severity=Severity.failure, message=error_description))

        if all_failed and results:
            logger.force_write()
            log_file = get_log_file()
            suffix = " Failed to get log file"
            if log_file:
                suffix = f"\nCheck the log file {cls._path_as_display_name(log_file).as_posix()}."
            raise ToolkitRepeatedUploadFailureError(
                f"Upload process was stopped due to repeatedly failed uploads.{suffix}"
            )
