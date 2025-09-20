from functools import partial
from pathlib import Path
from typing import Any

from cognite.client.data_classes._base import T_CogniteResourceList
from rich.console import Console

from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import FileReader
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
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
            executor = ProducerWorkerExecutor[list[dict[str, JsonVal]], T_CogniteResourceList](
                download_iterable=chunker(reader.read_chunks(), io.CHUNK_SIZE),
                process=io.json_chunk_to_data,
                write=partial(io.upload_items, selector=selector) if not dry_run else self._no_op,
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
            console.print(
                f"{final_action} {executor.total_items:,} {io.DISPLAY_NAME} from {file_display.as_posix()!r}{suffix}."
            )

    @staticmethod
    def _no_op(_: Any) -> None:
        """A no-operation function used when dry_run is True."""
        pass
