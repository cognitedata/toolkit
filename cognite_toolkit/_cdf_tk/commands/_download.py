from collections.abc import Iterable
from functools import partial
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResourceList
from rich.console import Console

from cognite_toolkit._cdf_tk.constants import DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import ConfigurableStorageIO, StorageIO, T_Selector, TableStorageIO
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.utils.file import safe_write, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import TABLE_WRITE_CLS_BY_FORMAT, Compression, FileWriter, SchemaColumn
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, JsonVal, T_WritableCogniteResourceList

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
        compression_cls = Compression.from_name(compression)

        console = Console()
        for selector in selectors:
            target_dir = output_dir / selector.group
            if verbose:
                console.print(f"Downloading {io.DISPLAY_NAME} '{selector!s}' to {target_dir.as_posix()!r}")

            iteration_count = self._get_iteration_count(io, selector, limit)
            filestem = str(selector)
            if self._already_downloaded(target_dir, filestem):
                warning = LowSeverityWarning(
                    f"Data for {selector!s} already exists in {target_dir.as_posix()!r}. Skipping download."
                )
                self.warn(warning, console=console)
                continue

            selector.dump_to_file(target_dir)
            columns: list[SchemaColumn] | None = None
            if file_format in TABLE_WRITE_CLS_BY_FORMAT and isinstance(io, TableStorageIO):
                columns = io.get_schema(selector)
            elif file_format in TABLE_WRITE_CLS_BY_FORMAT:
                raise ToolkitValueError(
                    f"Cannot download {io.KIND} in {file_format!r} format. The {io.KIND!r} storage type does not support table schemas."
                )

            with FileWriter.create_from_format(
                file_format, target_dir, io.KIND, compression_cls, columns=columns
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
                    write_description=f"Writing to {target_dir.as_posix()!r} in files with stem {filestem!r}",
                    console=console,
                )
                executor.run()
                executor.raise_on_error()
                file_count = writer.file_count

            if isinstance(io, ConfigurableStorageIO):
                for config in io.configurations(selector):
                    config_file = target_dir / DATA_RESOURCE_DIR / config.folder_name / f"{filestem}.{config.kind}.yaml"
                    config_file.parent.mkdir(parents=True, exist_ok=True)
                    safe_write(config_file, yaml_safe_dump(config.value))

            console.print(f"Downloaded {selector!s} to {file_count} file(s) in {target_dir.as_posix()!r}.")

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

    @staticmethod
    def _already_downloaded(output_dir: Path, filestem: str) -> bool:
        if not output_dir.exists():
            return False
        return any(output_dir.glob(f"{filestem}.*"))
