from collections import Counter
from collections.abc import Iterable
from functools import partial
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResourceList
from rich.console import Console

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import StorageIO, TableStorageIO
from cognite_toolkit._cdf_tk.utils.file import safe_write, to_directory_compatible, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import TABLE_WRITE_CLS_BY_FORMAT, Compression, FileWriter, SchemaColumn
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal, T_Selector, T_WritableCogniteResourceList

from ._base import ToolkitCommand


class DownloadCommand(ToolkitCommand):
    def download(
        self,
        selectors: Iterable[T_Selector],
        io: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        output_dir: Path,
        verbose: bool,
        file_format: str,
        compression: str,
        limit: int | None = 100_000,
    ) -> None:
        """Downloads data from CDF to the specified output directory.

        Args:
            selectors: The selectors of the resources to download.
            io: The StorageIO instance that defines how to download and process the data.
            output_dir: The directory where the downloaded files will be saved.
            verbose: If True, prints detailed information about the download process.
            file_format: The format of the files to be written (e.g., ".ndjson").
            compression: The compression method to use for the downloaded files (e.g., "none", "gzip").
            limit: The maximum number of items to download for each selected set. If None, all items will be downloaded.
        """
        target_directory = output_dir / io.FOLDER_NAME
        target_directory.mkdir(parents=True, exist_ok=True)
        compression_cls = Compression.from_name(compression)

        console = Console()
        filestem_counter: dict[str, int] = Counter()
        for selector in selectors:
            if verbose:
                console.print(f"Downloading {io.DISPLAY_NAME} '{selector!s}' to {target_directory.as_posix()!r}")

            filestem = to_directory_compatible(str(selector))
            if filestem_counter[filestem] > 0:
                filestem = f"{filestem}_{filestem_counter[filestem]}"
            filestem_counter[filestem] += 1
            iteration_count = self._get_iteration_count(io, selector, limit)

            columns: list[SchemaColumn] | None = None
            if file_format in TABLE_WRITE_CLS_BY_FORMAT and isinstance(io, TableStorageIO):
                columns = io.get_schema(selector)
            elif file_format in TABLE_WRITE_CLS_BY_FORMAT:
                raise ToolkitValueError(
                    f"Cannot download {io.KIND} in {file_format!r} format. The {io.KIND!r} storage type does not support table schemas."
                )

            with FileWriter.create_from_format(
                file_format, target_directory, io.KIND, compression_cls, columns=columns
            ) as writer:
                executor = ProducerWorkerExecutor[T_WritableCogniteResourceList, list[dict[str, JsonVal]]](
                    download_iterable=io.stream_data(selector, limit),
                    process=io.data_to_json_chunk,
                    write=partial(writer.write_chunks, filestem=filestem),
                    iteration_count=iteration_count,
                    # Limit queue size to avoid filling up memory before the workers can write to disk.
                    max_queue_size=8 * 10,  # 8 workers, 10 items per worker
                    download_description=f"Downloading {selector!s}",
                    process_description="Processing",
                    write_description=f"Writing to {target_directory.as_posix()!r} in files with stem {filestem!r}",
                    console=console,
                )
                executor.run()
                executor.raise_on_error()
                file_count = writer.file_count

            for config in io.configurations(selector):
                config_file = output_dir / config.folder_name / f"{filestem}.{config.kind}.yaml"
                config_file.parent.mkdir(parents=True, exist_ok=True)
                safe_write(config_file, yaml_safe_dump(config.value))

            console.print(f"Downloaded {selector!s} to {file_count} file(s) in {target_directory.as_posix()!r}.")

    @staticmethod
    def _get_iteration_count(
        io: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        selector: T_Selector,
        limit: int | None,
    ) -> int | None:
        total = io.count(selector)
        if total is not None and limit is not None and total > limit:
            total = limit
        iteration_count: int | None = None
        if total is not None:
            iteration_count = total // io.CHUNK_SIZE + (1 if total % io.CHUNK_SIZE > 0 else 0)
        return iteration_count
