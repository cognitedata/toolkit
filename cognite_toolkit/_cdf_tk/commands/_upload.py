from functools import partial
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResourceList
from rich.console import Console

from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import FileReader
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, ItemIDMessage, SuccessItem
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.progress_tracker import ProgressTracker
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal, T_Selector, T_WritableCogniteResourceList

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
        io: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        input_dir: Path,
        ensure_configurations: bool,
        dry_run: bool,
        verbose: bool,
    ) -> None:
        """Uploads data from files in the specified input directory to CDF.

        Args:
            io: The StorageIO instance that defines how to upload the data.
            input_dir: The directory containing the files to upload.
            ensure_configurations: If True, creates a configuration for the upload. For example,
                in the case of uploading RAW tables, this will create the RAW database and table.
                For asset-centric, this will create labels and data sets.
            dry_run: If True, performs a dry run without actually uploading the data.
            verbose: If True, prints detailed information about the upload process.

        """
        console = Console()
        cwd = Path.cwd()
        files = list(input_dir.glob(f"*.{io.KIND}.*"))
        if verbose:
            input_dir_display = input_dir
            if input_dir.is_relative_to(cwd):
                input_dir_display = input_dir.relative_to(cwd)
            console.print(f"Found {len(files)} files to upload in {input_dir_display.as_posix()!r}.")

        action = "Would upload" if dry_run else "Uploading"
        with HTTPClient(config=io.client.config) as upload_client:
            for file in files:
                file_display = file
                if file_display.is_relative_to(cwd):
                    file_display = file_display.relative_to(cwd)
                if verbose:
                    console.print(f"{action} {io.DISPLAY_NAME} from {file_display.as_posix()!r}")

                selector = io.load_selector(file)
                if ensure_configurations and not dry_run:
                    io.ensure_configurations(selector, console)

                reader = FileReader.from_filepath(file)
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
                    ),
                    iteration_count=None,
                    max_queue_size=self._MAX_QUEUE_SIZE,
                    download_description=f"Reading {file_display.as_posix()!s}",
                    process_description="Processing",
                    write_description=f"{action} {io.DISPLAY_NAME!r}",
                    console=console,
                )
                executor.run()
                executor.raise_on_error()
                final_action = "Uploaded" if not dry_run else "Would upload"
                suffix = " successfully" if not dry_run else ""
                results = tracker.aggregate()
                success = results.get((self._UPLOAD, "success"), 0)
                failed = results.get((self._UPLOAD, "failed"), 0)
                if failed > 0:
                    suffix += f", {failed:,} failed"
                console.print(f"{final_action} {success:,} {io.DISPLAY_NAME} from {file_display.as_posix()!r}{suffix}.")

    @classmethod
    def _upload_items(
        cls,
        data_chunk: T_CogniteResourceList,
        upload_client: HTTPClient,
        io: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        selector: T_Selector,
        dry_run: bool,
        tracker: ProgressTracker[T_ID],
    ) -> None:
        if dry_run:
            for item in data_chunk:
                tracker.set_progress(io.as_id(item), cls._UPLOAD, "success")
            return
        results = io.upload_items_force(data_chunk, upload_client, selector)
        for item in results:
            if isinstance(item, SuccessItem):
                tracker.set_progress(item.id, step=cls._UPLOAD, status="success")
            elif isinstance(item, ItemIDMessage):
                tracker.set_progress(item.id, step=cls._UPLOAD, status="failed")
