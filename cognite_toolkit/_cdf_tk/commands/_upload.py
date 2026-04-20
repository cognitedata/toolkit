import re
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from functools import partial
from pathlib import Path

from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsFailedRequest,
    ItemsFailedResponse,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewId
from cognite_toolkit._cdf_tk.constants import DATA_MANIFEST_SUFFIX, DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.dataio import (
    ChartIO,
    FileContentIO,
    FileMetadataContentIO,
    Page,
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
from cognite_toolkit._cdf_tk.dataio.selectors import Selector, load_selector
from cognite_toolkit._cdf_tk.dataio.selectors._instances import InstanceSpaceSelector, InstanceViewSelector
from cognite_toolkit._cdf_tk.exceptions import ToolkitRuntimeError, ToolkitValueError
from cognite_toolkit._cdf_tk.protocols import T_ResourceRequest, T_ResourceResponse
from cognite_toolkit._cdf_tk.resource_ios import ViewIO
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning, MediumSeverityWarning, ToolkitWarning
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.fileio import MultiFileReader, NDJsonWriter, Uncompressed
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import ToolkitCommand
from .deploy import DeployCommand


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
        └── ...
        """
        console = client.console
        data_files_by_selector = self._find_data_files(input_dir)

        self._deploy_resource_folder(input_dir / DATA_RESOURCE_DIR, deploy_resources, client, console, dry_run, verbose)

        data_files_by_selector = self._topological_sort_if_instance_selector(data_files_by_selector, client)

        total_file_count = sum(len(files) for files in data_files_by_selector.values())
        if verbose:
            input_dir_display = self._path_as_display_name(input_dir)
            console.print(f"Found {total_file_count} files to upload in {input_dir_display.as_posix()!r}.")
        self.upload_data(data_files_by_selector, input_dir, client, dry_run, console, verbose, skip_strict_mode)

    def _topological_sort_if_instance_selector(
        self, data_files_by_selector: dict[Selector, list[Path]], client: ToolkitClient
    ) -> dict[Selector, list[Path]]:
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

        view_dependencies, cyclic_views = ViewIO.create_loader(client).topological_sort_container_constraints(
            list(selector_by_view_id.keys())
        )
        if cyclic_views:
            MediumSeverityWarning(
                f"Cyclic dependencies detected between views: {', '.join(str(view) for view in cyclic_views)}. "
                f"If their constraints are not already satisfied by instances in existing containers / views "
                f"in CDF, the upload may fail for these views."
            ).print_warning(console=client.console)
            view_dependencies = view_dependencies + cyclic_views

        prepared_selectors: dict[Selector, list[Path]] = {}

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
    ) -> dict[Selector, list[Path]]:
        """Finds data files and their corresponding metadata files in the input directory."""
        data_files_by_metadata: dict[Selector, list[Path]] = {}
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
        return data_files_by_metadata

    def _deploy_resource_folder(
        self,
        resource_dir: Path,
        deploy_resources: bool,
        client: ToolkitClient,
        console: Console,
        dry_run: bool,
        verbose: bool,
    ) -> None:
        """Deploy resources from the specified resource directory if it exists and deployment is enabled."""
        if deploy_resources and resource_dir.exists():
            deploy_command = DeployCommand()
            deploy_results = deploy_command.deploy_all_resources(
                client=client,
                build_dir=resource_dir,
                env_vars=EnvironmentVariables.create_from_environment(),
                dry_run=dry_run,
                verbose=verbose,
            )
            self.warning_list.extend(deploy_command.warning_list)
            if deploy_results.has_counts:
                console.print(deploy_results.counts_table())
        elif deploy_resources:
            self.warn(
                MediumSeverityWarning(
                    f"Resource directory {resource_dir.as_posix()!r} does not exist, skipping resource deployment."
                )
            )

    @classmethod
    def upload_data(
        cls,
        data_files_by_selector: Mapping[Selector, list[Path]],
        input_dir: Path,
        client: ToolkitClient,
        dry_run: bool,
        console: Console,
        verbose: bool,
        skip_strict_mode: bool = False,
    ) -> None:
        action = "Would upload" if dry_run else "Uploading"

        input_dir.mkdir(parents=True, exist_ok=True)
        log_filestem = cls._create_upload_logfile_stem(input_dir)
        with (
            NDJsonWriter(
                input_dir, kind="UploadIssues", default_filestem=log_filestem, compression=Uncompressed
            ) as log_file,
            FileWithAggregationLogger(log_file) as logger,
            HTTPClient(config=client.config) as upload_client,
        ):
            for selector, datafiles in data_files_by_selector.items():
                io = cls._create_selected_io(selector, datafiles[0], client, skip_strict_mode)
                if io is None:
                    continue
                io.logger = logger
                logger.reset()
                schema = io.get_schema(selector) if isinstance(io, TableDataIO) else None
                reader = MultiFileReader(datafiles, schema=schema)
                # FileContentIO supports uploading any file format.
                if reader.is_table and not isinstance(io, TableUploadableDataIO | FileContentIO):
                    raise ToolkitValueError(
                        f"{selector.type}.{selector.kind} does not support {reader.format!r} files."
                    )

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
                items_results = logger.finalize(dry_run)
                display_item_results(items_results, title=f"Finished upload {selector.display_name}", console=console)
                executor.raise_on_error()

    @staticmethod
    def _create_upload_logfile_stem(log_dir: Path) -> str:
        """Create a filestem for the upload log file that does not conflict with existing files in the directory."""
        base_logstem = "upload-"
        existing_files = list(log_dir.glob(f"{base_logstem}*"))
        if not existing_files:
            return base_logstem

        run_pattern = re.compile(re.escape(base_logstem) + r"run(\d+)-")
        max_run = 0
        for f in existing_files:
            match = run_pattern.match(f.name)
            if match:
                max_run = max(max_run, int(match.group(1)))

        next_run = max(2, max_run + 1)
        return f"{base_logstem}run{next_run}-"

    @staticmethod
    def _path_as_display_name(input_path: Path, cwd: Path = Path.cwd()) -> Path:
        display_name = input_path
        if input_path.is_relative_to(cwd):
            display_name = input_path.relative_to(cwd)
        return display_name

    @classmethod
    def _create_selected_io(
        cls, selector: Selector, data_file: Path, client: ToolkitClient, skip_strict_mode: bool
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
        elif issubclass(io_cls, FileMetadataContentIO):
            return FileMetadataContentIO(client, config_directory=data_file.parent)
        else:
            return io_cls(client)

    @classmethod
    def _upload_items(
        cls,
        data_chunk: Page[T_ResourceRequest],
        upload_client: HTTPClient,
        io: UploadableDataIO[T_Selector, T_ResourceResponse, T_ResourceRequest],
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
            logger.apply_to_all_unprocessed(
                label="Early termination of upload process",
                severity=Severity.skipped,
            )
            logger.force_write()
            log_file = get_log_file()
            suffix = " Failed to get log file"
            if log_file:
                suffix = f"\nCheck the log file {cls._path_as_display_name(log_file).as_posix()}."
            raise ToolkitRuntimeError(f"Upload process was stopped due to repeatedly failed uploads.{suffix}")
