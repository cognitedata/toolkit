from functools import partial
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResourceList
from pydantic import ValidationError
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.constants import DATA_METADATA_STEM, DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.data_classes import DeployResults
from cognite_toolkit._cdf_tk.storageio import StorageIO, T_Selector
from cognite_toolkit._cdf_tk.storageio.selectors import SelectorAdapter
from cognite_toolkit._cdf_tk.tk_warnings import MediumSeverityWarning
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.utils.fileio import FileReader
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ItemIDMessage, SuccessItem
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.progress_tracker import ProgressTracker
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.validation import humanize_validation_error

from . import DeployCommand
from ._base import ToolkitCommand


class UploadCommand(ToolkitCommand):
    """Command for uploading data to CDF from files in a specified directory.

    This command reads files matching a specific pattern in the input directory,
    processes them using a StorageIO instance, and uploads the data to CDF.

    Attributes:
        _MAX_QUEUE_SIZE (int): The maximum size of the queue for processing items.
            Set to 80 to balance memory usage and processing speed.
    """

    _MAX_QUEUE_SIZE = 80
    _UPLOAD = "upload"

    def upload(
        self,
        input_dir: Path,
        client: ToolkitClient,
        deploy_resources: bool,
        dry_run: bool,
        verbose: bool,
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
        ├── datafile1.Metadata.yaml       # Metadata file for datafile1
        ├── datafile2.kind2.ndjson # Another data file of the same or different kind
        ├── datafile2.Metadata.yaml       # Metadata file for datafile2
        └── ...
        """
        console = Console()
        data_files_by_selector = self._find_data_files(input_dir, None)

        total_file_count = sum(len(files) for files in data_files_by_selector.values())
        if verbose:
            input_dir_display = self._path_as_display_name(input_dir)
            console.print(f"Found {total_file_count} files to upload in {input_dir_display.as_posix()!r}.")

        resource_dir = input_dir / DATA_RESOURCE_DIR
        if deploy_resources and resource_dir.exists():
            deploy_command = DeployCommand()
            results = DeployResults([], "deploy", dry_run=dry_run)
            deploy_command.deploy_all_resources(
                client=client,
                results=results,
                build=BuildEnvironment,
                build_dir=resource_dir,
                drop=False,
                drop_data=False,
                dry_run=dry_run,
            )
            self.warning_list.extend(deploy_command.warning_list)
        elif deploy_resources:
            self.warn(
                MediumSeverityWarning(
                    f"Resource directory {resource_dir.as_posix()!r} does not exist, skipping resource deployment."
                )
            )

        action = "Would upload" if dry_run else "Uploading"
        with HTTPClient(config=client.config) as upload_client:
            file_count = 1
            for selector, datafiles in data_files_by_selector.items():
                io = self._create_selected_io(client, selector)
                for data_file in datafiles:
                    file_display = self._path_as_display_name(data_file)
                    if verbose:
                        console.print(f"{action} {io.DISPLAY_NAME} from {file_display.as_posix()!r}")

                    reader = FileReader.from_filepath(data_file)
                    tracker = ProgressTracker[T_ID]([self._UPLOAD])
                    executor = ProducerWorkerExecutor[list[dict[str, JsonVal]], T_CogniteResourceList](
                        download_iterable=chunker(reader.read_chunks(), io.CHUNK_SIZE),
                        process=io.json_chunk_to_data,
                        write=partial(
                            self._upload_items,
                            upload_client=upload_client,
                            io=io,
                            dry_run=dry_run,
                            selector=selector,
                            tracker=tracker,
                            console=console,
                        ),
                        iteration_count=None,
                        max_queue_size=self._MAX_QUEUE_SIZE,
                        download_description=f"Reading {file_count:,}/{total_file_count + 1:,}: {file_display.as_posix()!s}",
                        process_description="Processing",
                        write_description=f"{action} {io.DISPLAY_NAME!r}",
                        console=console,
                    )
                    executor.run()
                    file_count += 1
                    executor.raise_on_error()
                    final_action = "Uploaded" if not dry_run else "Would upload"
                    suffix = " successfully" if not dry_run else ""
                    results = tracker.aggregate()
                    success = results.get((self._UPLOAD, "success"), 0)
                    failed = results.get((self._UPLOAD, "failed"), 0)
                    if failed > 0:
                        suffix += f", {failed:,} failed"
                    console.print(
                        f"{final_action} {success:,} {io.DISPLAY_NAME} from {file_display.as_posix()!r}{suffix}."
                    )

    @staticmethod
    def _path_as_display_name(input_path: Path, cwd: Path = Path.cwd()) -> Path:
        display_name = input_path
        if input_path.is_relative_to(cwd):
            display_name = input_path.relative_to(cwd)
        return display_name

    def _find_data_files(
        self,
        input_dir: Path,
        io: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList] | None,
    ) -> dict[T_Selector, list[Path]]:
        metadata_file_endswith = f".{DATA_METADATA_STEM}.yaml"
        data_files_by_metadata: dict[T_Selector, list[Path]] = {}
        for metadata_file in input_dir.glob(f"*{metadata_file_endswith}"):
            data_file_prefix = metadata_file.name.removesuffix(metadata_file_endswith)
            data_files = list(input_dir.glob(f"{data_file_prefix}.*"))
            if io is not None and data_files:
                data_files = [data_file for data_file in data_files if data_file.stem.endswith(io.KIND)]
                if not data_files:
                    continue
            if not data_files:
                self.warn(
                    MediumSeverityWarning(
                        f"Metadata file {metadata_file.as_posix()!r} has no corresponding data files, skipping.",
                    )
                )
                continue

            selector_dict = read_yaml_file(metadata_file, expected_output="dict")
            try:
                selector = SelectorAdapter.validate_python(selector_dict)
            except ValidationError as e:
                errors = humanize_validation_error(e)
                self.warn(
                    ResourceFormatWarning(
                        metadata_file, tuple(errors), text="Invalid selector in metadata file, skipping."
                    )
                )
                continue
            data_files_by_metadata[selector] = data_files
        return data_files_by_metadata

    @classmethod
    def _upload_items(
        cls,
        data_chunk: T_CogniteResourceList,
        upload_client: HTTPClient,
        io: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        selector: T_Selector,
        dry_run: bool,
        tracker: ProgressTracker[T_ID],
        console: Console,
    ) -> None:
        if dry_run:
            for item in data_chunk:
                tracker.set_progress(io.as_id(item), cls._UPLOAD, "success")
            return
        results = io.upload_items(data_chunk, upload_client, selector)
        for item in results:
            if isinstance(item, SuccessItem):
                tracker.set_progress(item.id, step=cls._UPLOAD, status="success")
            elif isinstance(item, ItemIDMessage):
                tracker.set_progress(item.id, step=cls._UPLOAD, status="failed")
            else:
                console.log(f"[red]Unexpected result from upload: {str(item)!r}[/red]")
