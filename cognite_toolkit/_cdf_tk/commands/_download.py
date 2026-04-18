from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date
from functools import partial
from pathlib import Path
from typing import Generic

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


@dataclass
class DownloadStep(Generic[T_Selector]):
    selector: T_Selector
    count: int | None
    filestem: str
    target_dir: Path
    schema: list[SchemaColumn] | None
    is_table: bool
    limit: int | None

    @property
    def download_count(self) -> int | None:
        if self.limit is not None and self.count is not None:
            return min(self.limit, self.count)
        return self.count

    @property
    def skip_message(self) -> str | None:
        if self.count == 0:
            return f"No items to download for {self.selector!s}. Skipping."
        elif self._already_downloaded():
            return f"Data for {self.selector!s} already exists in {self.target_dir.as_posix()!r}. Skipping download."
        return None

    def _already_downloaded(self) -> bool:
        if not self.target_dir.exists():
            return False

        # Check for multi-part files (e.g. ndjson, csv, parquet)
        if any(self.target_dir.glob(f"{self.filestem}-part-*")):
            return True

        # Check for single files (e.g. yaml) and exclude the metadata file.
        manifest_file_name = f"{self.filestem}.{DATA_MANIFEST_STEM}.yaml"
        for f in self.target_dir.glob(f"{self.filestem}.*"):
            if f.name != manifest_file_name:
                return True

        return False


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
        console = io.client.console
        plan = self._create_plan(io, selectors, output_dir, file_format, limit)
        self._display_plan(plan, console)

        for step in plan:
            if skip_message := step.skip_message:
                console.print(skip_message)
                continue
            elif verbose:
                console.print(
                    f"Downloading {step.selector.display_name} '{step.selector!s}' to {step.target_dir.as_posix()!r}"
                )

            step.selector.dump_to_file(step.target_dir)

            with (
                self._create_data_file_writer(step, file_format, compression) as writer,
                self._create_log_file_writer(step.target_dir) as log_file,
                FileWithAggregationLogger(log_file) as logger,
            ):
                file_count = self._download_data(io, step, writer, logger, console)
                if isinstance(io, ConfigurableDataIO):
                    self._dump_configuration(io, step)

            console.print(f"Downloaded {step.selector!s} to {file_count} file(s) in {step.target_dir.as_posix()!r}.")

    @classmethod
    def _create_plan(
        cls,
        io: DataIO[T_Selector, T_ResourceResponse],
        selectors: Sequence[T_Selector],
        output_dir: Path,
        file_format: str,
        limit: int | None,
    ) -> list[DownloadStep[T_Selector]]:
        plan: list[DownloadStep[T_Selector]] = []
        for selector in selectors:
            count = io.count(selector)
            target_dir = cls._get_target_dir(selector, output_dir)

            filestem = sanitize_filename(str(selector))
            columns = cls._get_columns(io, selector, file_format)
            is_table = file_format in TABLE_WRITE_CLS_BY_FORMAT
            plan.append(DownloadStep(selector, count, filestem, target_dir, columns, is_table, limit))
        return plan

    @classmethod
    def _display_plan(cls, plan: list[DownloadStep[T_Selector]], console: Console) -> None:
        table = Table(title="Planned Downloads")
        table.add_column("Data Type", style="cyan")
        table.add_column("Item Count", justify="right", style="green")
        for step in plan:
            download_count = step.download_count
            if (
                step.limit is not None
                and step.count is not None
                and download_count is not None
                and step.count > step.limit
            ):
                display_value = f"{download_count:,} (of {step.count:,} available)"
            else:
                display_value = f"{download_count:,}" if download_count is not None else "Unknown"

            table.add_row(str(step.selector), display_value)
        console.print(table)

    @classmethod
    def _get_target_dir(cls, selector: T_Selector, output_dir: Path) -> Path:
        if selector.download_dir_name is None:
            raise NotImplementedError(f"Bug in Toolkit. The download_dir_name field is missing for {selector!r}.")
        return output_dir / sanitize_filename(selector.download_dir_name)

    @classmethod
    def _get_columns(
        cls, io: DataIO[T_Selector, T_ResourceResponse], selector: T_Selector, file_format: str
    ) -> list[SchemaColumn] | None:
        columns: list[SchemaColumn] | None = None
        is_table = file_format in TABLE_WRITE_CLS_BY_FORMAT
        if is_table and isinstance(io, TableDataIO):
            columns = io.get_schema(selector)
        elif is_table:
            raise ToolkitValueError(
                f"Cannot download {selector.kind} in {file_format!r} format. The {selector.kind!r} data type does not support table schemas."
            )
        return columns

    @classmethod
    def _create_data_file_writer(cls, step: DownloadStep[T_Selector], file_format: str, compression: str) -> FileWriter:
        return FileWriter.create_from_format(
            file_format, step.target_dir, step.selector.kind, Compression.from_name(compression), columns=step.schema
        )

    @classmethod
    def _create_log_file_writer(cls, target_dir: Path) -> NDJsonWriter:
        log_filestem = f"download_{date.today().strftime('%Y%m%d')}"
        return NDJsonWriter(target_dir, kind="DownloadLogs", default_filestem=log_filestem, compression=Uncompressed)

    @classmethod
    def _download_data(
        cls,
        io: DataIO[T_Selector, T_ResourceResponse],
        step: DownloadStep[T_Selector],
        writer: FileWriter,
        logger: FileWithAggregationLogger,
        console: Console,
    ) -> int:
        io.logger = logger
        logger.reset()
        executor = ProducerWorkerExecutor[Page[T_ResourceResponse], Page[dict[str, JsonVal]]](
            download_iterable=io.stream_data(step.selector, step.limit),
            process=cls.create_data_process(io=io, selector=step.selector, is_table=step.is_table),
            write=cls.create_writer(writer, step.filestem),
            total_item_count=step.count,
            # Limit queue size to avoid filling up memory before the workers can write to disk.
            max_queue_size=8 * 10,  # 8 workers, 10 items per worker
            download_description=f"Downloading {step.selector!s}",
            process_description="Processing",
            write_description=f"Writing to {step.target_dir.as_posix()!r} in files with stem {step.filestem!r}",
            console=console,
        )
        executor.run()

        items_results = logger.finalize(is_dry_run=False)
        display_item_results(items_results, title=f"Finished {step.selector.display_name}", console=console)
        executor.raise_on_error()
        return writer.file_count

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
        io: ConfigurableDataIO[T_Selector, T_ResourceResponse], step: DownloadStep[T_Selector]
    ) -> None:
        for config in io.configurations(step.selector):
            filename = config.filename or step.filestem
            config_file = step.target_dir / DATA_RESOURCE_DIR / config.folder_name / f"{filename}.{config.kind}.yaml"
            config_file.parent.mkdir(parents=True, exist_ok=True)
            safe_write(config_file, yaml_safe_dump(config.value))
