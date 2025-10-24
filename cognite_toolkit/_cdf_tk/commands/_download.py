from collections.abc import Iterable
from functools import partial
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResource
from rich.console import Console

from cognite_toolkit._cdf_tk.constants import DATA_MANIFEST_STEM, DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import ConfigurableStorageIO, Page, StorageIO, T_Selector, TableStorageIO
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.utils.file import safe_write, sanitize_filename, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import TABLE_WRITE_CLS_BY_FORMAT, Compression, FileWriter, SchemaColumn
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import ToolkitCommand


class DownloadCommand(ToolkitCommand):
    def download(
        self,
        selectors: Iterable[T_Selector],
        io: StorageIO[T_Selector, T_CogniteResource],
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
                console.print(f"Downloading {selector.display_name} '{selector!s}' to {target_dir.as_posix()!r}")

            iteration_count = self._get_iteration_count(io, selector, limit)
            filestem = sanitize_filename(str(selector))
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
                    f"Cannot download {selector.kind} in {file_format!r} format. The {selector.kind!r} storage type does not support table schemas."
                )

            with FileWriter.create_from_format(
                file_format, target_dir, selector.kind, compression_cls, columns=columns
            ) as writer:
                executor = ProducerWorkerExecutor[Page[T_CogniteResource], list[dict[str, JsonVal]]](
                    download_iterable=io.stream_data(selector, limit),
                    process=partial(self.process_data_chunk, io=io),
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
                    filename = config.filename or filestem
                    config_file = target_dir / DATA_RESOURCE_DIR / config.folder_name / f"{filename}.{config.kind}.yaml"
                    config_file.parent.mkdir(parents=True, exist_ok=True)
                    safe_write(config_file, yaml_safe_dump(config.value))

            console.print(f"Downloaded {selector!s} to {file_count} file(s) in {target_dir.as_posix()!r}.")

    @staticmethod
    def _get_iteration_count(
        io: StorageIO[T_Selector, T_CogniteResource],
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

        # Check for multi-part files (e.g. ndjson, csv, parquet)
        if any(output_dir.glob(f"{filestem}-part-*")):
            return True

        # Check for single files (e.g. yaml) and exclude the metadata file.
        manifest_file_name = f"{filestem}.{DATA_MANIFEST_STEM}.yaml"
        for f in output_dir.glob(f"{filestem}.*"):
            if f.name != manifest_file_name:
                return True

        return False

    @staticmethod
    def process_data_chunk(
        data_page: Page[T_CogniteResource],
        io: StorageIO[T_Selector, T_CogniteResource],
    ) -> list[dict[str, JsonVal]]:
        """Processes a chunk of data by converting it to a JSON-compatible format.

        Args:
            data_page: The page of data to process.
            io: The StorageIO instance that defines how to process the data.

        Returns:
            A list of dictionaries representing the processed data in a JSON-compatible format.
        """
        return io.data_to_json_chunk(data_page.items)
