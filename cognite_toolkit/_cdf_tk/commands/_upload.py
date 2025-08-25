from functools import partial
from pathlib import Path
from typing import Any

from rich.console import Console

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.storageio._base import T_CogniteResourceList, T_StorageID, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import FileReader
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

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
        io: StorageIO[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList],
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
        files = list(input_dir.glob(f"*.{io.kind}.*"))
        if verbose:
            console.print(f"Found {len(files)} files to upload in {input_dir.as_posix()!r}.")

        action = "Would upload" if dry_run else "Uploading"
        for file in files:
            if verbose:
                console.print(f"{action} {io.display_name} from {file.as_posix()!r}")

            identifier = io.load_identifier(file)
            if ensure_configurations and not dry_run:
                io.ensure_configurations(identifier, console)

            reader = FileReader.from_filepath(file)
            executor = ProducerWorkerExecutor[list[dict[str, JsonVal]], T_CogniteResourceList](
                download_iterable=chunker(reader.read_chunks(), io.chunk_size),
                process=io.json_chunk_to_data,
                write=partial(io.upload_items, identifier=identifier) if not dry_run else self._no_op,
                iteration_count=None,
                max_queue_size=self._MAX_QUEUE_SIZE,
                download_description=f"Reading {file.as_posix()!s}",
                process_description="Processing",
                write_description=f"{action} {io.display_name!r}",
                console=console,
            )
            executor.run()
            if executor.error_occurred:
                raise ToolkitValueError("An error occurred during the upload process: " + executor.error_message)
            elif executor.stopped_by_user:
                raise ToolkitValueError("The upload process was stopped by the user.")
            else:
                action = "Would upload" if dry_run else "Uploaded"
                console.print(f"{action} {file.as_posix()!r} successfully.")

    @staticmethod
    def _no_op(_: Any) -> None:
        """A no-operation function used when dry_run is True."""
        pass
