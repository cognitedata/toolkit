from functools import partial
from pathlib import Path

from rich.console import Console

from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.storageio._base import T_CogniteResourceList, T_StorageID, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.collection import chunker
from cognite_toolkit._cdf_tk.utils.fileio import FileReader
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import ToolkitCommand


class UploadCommand(ToolkitCommand):
    def upload(
        self,
        io: StorageIO[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList],
        input_dir: Path,
        verbose: bool,
    ) -> None:
        console = Console()
        files = input_dir.glob(f"*.{io.kind}.*")
        if verbose:
            console.print(f"Found {len(list(files))} files to upload in {input_dir.as_posix()!r}.")

        console = Console()
        for file in files:
            if verbose:
                console.print(f"Uploading {io.display_name} from {file.as_posix()!r}")

            identifier = self._get_identifier(io, file)

            reader = FileReader.from_filepath(file)
            executor = ProducerWorkerExecutor[list[dict[str, JsonVal]], T_CogniteResourceList](
                download_iterable=chunker(reader.read_chunks(), io.chunk_size),
                process=io.json_chunk_to_data,
                write=partial(io.upload_items, identifier=identifier),
                iteration_count=None,
                max_queue_size=8 * 10,
                download_description=f"Reading {file.as_posix()!s}",
                process_description="Processing",
                write_description=f"Uploading {io.display_name!r}",
                console=console,
            )
            executor.run()
            if executor.error_occurred:
                raise ValueError("An error occurred during the upload process: " + executor.error_message)
            console.print(f"Uploaded {file.as_posix()!r} successfully.")

    @staticmethod
    def _get_identifier(
        io: StorageIO[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList], file: Path
    ) -> T_StorageID:
        raise NotImplementedError()
