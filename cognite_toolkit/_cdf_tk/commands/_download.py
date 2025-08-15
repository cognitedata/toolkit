from collections.abc import Iterable
from functools import partial
from pathlib import Path

from rich.console import Console

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.storageio._base import T_CogniteResourceList, T_StorageID, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.file import safe_write, to_directory_compatible, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import Compression, FileWriter
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import ToolkitCommand


class DownloadCommand(ToolkitCommand):
    def download(
        self,
        identifiers: Iterable[T_StorageID],
        io: StorageIO[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList],
        output_dir: Path,
        verbose: bool,
        file_format: str,
        compression: str,
        limit: int | None = 100_000,
    ) -> None:
        """Downloads data from CDF to the specified output directory.

        Args:
            identifiers: The identifiers of the resources to download.
            io: The StorageIO instance that defines how to download and process the data.
            output_dir: The directory where the downloaded files will be saved.
            verbose: If True, prints detailed information about the download process.
            file_format: The format of the files to be written (e.g., ".ndjson").
            compression: The compression method to use for the downloaded files (e.g., "none", "gzip").
            limit: The maximum number of items to download for each identifier. If None, all items will be downloaded.
        """
        target_directory = output_dir / io.folder_name
        target_directory.mkdir(parents=True, exist_ok=True)
        compression_cls = Compression.from_name(compression)

        console = Console()
        for identifier in identifiers:
            if verbose:
                console.print(f"Downloading {io.display_name} '{identifier!s}' to {target_directory.as_posix()!r}")

            filestem = to_directory_compatible(str(identifier))
            iteration_count = self._get_iteration_count(io, identifier, limit)

            with FileWriter.create_from_format(file_format, target_directory, io.kind, compression_cls) as writer:
                executor = ProducerWorkerExecutor[T_WritableCogniteResourceList, list[dict[str, JsonVal]]](
                    download_iterable=io.download_iterable(identifier, limit),
                    process=io.data_to_json_chunk,
                    write=partial(writer.write_chunks, filestem=filestem),
                    iteration_count=iteration_count,
                    # Limit queue size to avoid filling up memory before the workers can write to disk.
                    max_queue_size=8 * 10,  # 8 workers, 10 items per worker
                    download_description=f"Downloading {identifier!s}",
                    process_description="Processing",
                    write_description=f"Writing to {target_directory.as_posix()!r} in files with stem {filestem!r}",
                    console=console,
                )
                executor.run()

                if executor.error_occurred:
                    raise ToolkitValueError(f"An error occurred during the download process: {executor.error_message}")
                file_count = writer.file_count

            for config in io.configurations(identifier):
                config_file = output_dir / config.folder_name / f"{filestem}.{config.kind}.yaml"
                config_file.parent.mkdir(parents=True, exist_ok=True)
                safe_write(config_file, yaml_safe_dump(config.value))

            console.print(f"Downloaded {identifier!s} to {file_count} file(s) in {target_directory.as_posix()!r}.")

    @staticmethod
    def _get_iteration_count(
        io: StorageIO[T_StorageID, T_CogniteResourceList, T_WritableCogniteResourceList],
        identifier: T_StorageID,
        limit: int | None,
    ) -> int | None:
        total = io.count(identifier)
        if total is not None and limit is not None and total > limit:
            total = limit
        iteration_count: int | None = None
        if total is not None:
            iteration_count = total // io.chunk_size + (1 if total % io.chunk_size > 0 else 0)
        return iteration_count
