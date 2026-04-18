from collections.abc import Callable, Sequence
from datetime import date
from functools import partial
from pathlib import Path

from rich.console import Console
from rich.table import Table

from cognite_toolkit._cdf_tk.constants import DATA_MANIFEST_STEM, DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.dataio import (
    ConfigurableDataIO,
    DataIO,
    Page,
    T_Selector,
    TableDataIO,
)
from cognite_toolkit._cdf_tk.dataio.logger import FileWithAggregationLogger, display_item_results
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.protocols import T_ResourceResponse
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.utils.file import safe_write, sanitize_filename, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import (
    TABLE_WRITE_CLS_BY_FORMAT,
    Compression,
    FileWriter,
    NDJsonWriter,
    SchemaColumn,
    Uncompressed,
)
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import ToolkitCommand


class DownloadCommand(ToolkitCommand):
    def download(
        self,
        selectors: Sequence[T_Selector],
        io: DataIO[T_Selector, T_ResourceResponse],
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
        console = io.client.console
        counts_by_selector = self._create_and_print_plan(io, selectors, console)

        for selector in selectors:
            target_dir = self._get_target_dir(selector, output_dir, console, verbose)

            filestem = sanitize_filename(str(selector))
            total = counts_by_selector[selector]
            if total == 0:
                console.print(f"No items to download for {selector!s}. Skipping.")
                continue
            elif self._already_downloaded(target_dir, filestem):
                self.warn(
                    LowSeverityWarning(
                        f"Data for {selector!s} already exists in {target_dir.as_posix()!r}. Skipping download."
                    ),
                    console=console,
                )
                continue

            selector.dump_to_file(target_dir)

            columns = self._get_columns(selector, file_format, io)

            log_filestem = f"download_{date.today().strftime('%Y%m%d')}"
            with (
                FileWriter.create_from_format(
                    file_format, target_dir, selector.kind, compression_cls, columns=columns
                ) as writer,
                NDJsonWriter(
                    target_dir, kind="DownloadLogs", default_filestem=log_filestem, compression=Uncompressed
                ) as log_file,
                FileWithAggregationLogger(log_file) as logger,
            ):
                file_count = self._download_data(
                    io,
                    logger,
                    selector,
                    writer,
                    filestem,
                    target_dir,
                    limit=limit,
                    is_table=file_format in TABLE_WRITE_CLS_BY_FORMAT,
                    total_item_count=total,
                    console=console,
                )
                if isinstance(io, ConfigurableDataIO):
                    self._dump_configuration(io, selector, filestem, target_dir)

            console.print(f"Downloaded {selector!s} to {file_count} file(s) in {target_dir.as_posix()!r}.")

    def _get_target_dir(self, selector: T_Selector, output_dir: Path, console: Console, verbose: bool) -> Path:
        if selector.download_dir_name is None:
            raise NotImplementedError(f"Bug in Toolkit. The download_dir_name field is missing for {selector!r}.")
        target_dir = output_dir / sanitize_filename(selector.download_dir_name)

        if verbose:
            console.print(f"Downloading {selector.display_name} '{selector!s}' to {target_dir.as_posix()!r}")
        return target_dir

    def _get_columns(
        self, selector: T_Selector, file_format: str, io: DataIO[T_Selector, T_ResourceResponse]
    ) -> list[SchemaColumn] | None:
        columns: list[SchemaColumn] | None = None
        is_table = file_format in TABLE_WRITE_CLS_BY_FORMAT
        if is_table and isinstance(io, TableDataIO):
            columns = io.get_schema(selector)
        elif is_table:
            raise ToolkitValueError(
                f"Cannot download {selector.kind} in {file_format!r} format. The {selector.kind!r} storage type does not support table schemas."
            )
        return columns

    @classmethod
    def _create_and_print_plan(
        cls, io: DataIO[T_Selector, T_ResourceResponse], selectors: Sequence[T_Selector], console: Console
    ) -> dict[T_Selector, int | None]:
        counts_by_selector: dict[T_Selector, int | None] = {}
        table = Table(title="Planned Downloads")
        table.add_column("Data Type", style="cyan")
        table.add_column("Item Count", justify="right", style="green")
        for selector in selectors:
            total = io.count(selector)
            counts_by_selector[selector] = total
            item_count = str(total) if total is not None else "Unknown"
            table.add_row(str(selector), item_count)
        console.print(table)
        return counts_by_selector

    @classmethod
    def _download_data(
        cls,
        io: DataIO[T_Selector, T_ResourceResponse],
        logger: FileWithAggregationLogger,
        selector: T_Selector,
        writer: FileWriter,
        filestem: str,
        target_dir: Path,
        limit: int | None,
        is_table: bool,
        total_item_count: int | None,
        console: Console,
    ) -> int:
        io.logger = logger
        executor = ProducerWorkerExecutor[Page[T_ResourceResponse], Page[dict[str, JsonVal]]](
            download_iterable=io.stream_data(selector, limit),
            process=cls.create_data_process(io=io, selector=selector, is_table=is_table),
            write=cls.create_writer(writer, filestem),
            total_item_count=total_item_count,
            # Limit queue size to avoid filling up memory before the workers can write to disk.
            max_queue_size=8 * 10,  # 8 workers, 10 items per worker
            download_description=f"Downloading {selector!s}",
            process_description="Processing",
            write_description=f"Writing to {target_dir.as_posix()!r} in files with stem {filestem!r}",
            console=console,
        )
        executor.run()

        items_results = logger.finalize(is_dry_run=False)
        display_item_results(items_results, title=f"Finished {selector.display_name}", console=console)
        executor.raise_on_error()
        return writer.file_count

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
    def create_data_process(
        io: DataIO[T_Selector, T_ResourceResponse],
        selector: T_Selector,
        is_table: bool,
    ) -> Callable[[Page[T_ResourceResponse]], Page[dict[str, JsonVal]]]:
        """Creates a data processing function based on the IO type and whether the output is a table."""
        if is_table and isinstance(io, TableDataIO):
            return partial(io.data_to_row, selector=selector)
        return partial(io.data_to_json_chunk, selector=selector)

    @classmethod
    def create_writer(cls, writer: FileWriter, filestem: str) -> Callable[[Page[dict[str, JsonVal]]], None]:
        """Creates a writer function that writes processed data to files using the provided FileWriter."""

        def write(page: Page[dict[str, JsonVal]]) -> None:
            writer.write_chunks(page.as_raw_items(), filestem=filestem)  # type: ignore[arg-type]

        return write

    @staticmethod
    def _dump_configuration(
        io: ConfigurableDataIO[T_Selector, T_ResourceResponse], selector: T_Selector, filestem: str, target_dir: Path
    ) -> None:
        for config in io.configurations(selector):
            filename = config.filename or filestem
            config_file = target_dir / DATA_RESOURCE_DIR / config.folder_name / f"{filename}.{config.kind}.yaml"
            config_file.parent.mkdir(parents=True, exist_ok=True)
            safe_write(config_file, yaml_safe_dump(config.value))
