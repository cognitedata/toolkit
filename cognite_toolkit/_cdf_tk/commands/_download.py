from collections.abc import Callable, Sequence
from functools import partial
from pathlib import Path

from rich.table import Table

from cognite_toolkit._cdf_tk.constants import DATA_MANIFEST_STEM, DATA_RESOURCE_DIR
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.protocols import T_ResourceResponse
from cognite_toolkit._cdf_tk.storageio import (
    ConfigurableStorageIO,
    Page,
    StorageIO,
    T_Selector,
    TableStorageIO,
)
from cognite_toolkit._cdf_tk.storageio.progress import Bookmark, ProgressYAML
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning
from cognite_toolkit._cdf_tk.utils.file import safe_write, sanitize_filename, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import (
    TABLE_WRITE_CLS_BY_FORMAT,
    Compression,
    FileWriter,
    SchemaColumn,
)
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from ._base import ToolkitCommand


class DownloadCommand(ToolkitCommand):
    def download(
        self,
        selectors: Sequence[T_Selector],
        io: StorageIO[T_Selector, T_ResourceResponse],
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

        for selector in selectors:
            if selector.download_dir_name is None:
                raise NotImplementedError(f"Bug in Toolkit. The download_dir_name field is missing for {selector!r}.")
            target_dir = output_dir / sanitize_filename(selector.download_dir_name)
            if verbose:
                console.print(f"Downloading {selector.display_name} '{selector!s}' to {target_dir.as_posix()!r}")

            total = counts_by_selector[selector]
            if total == 0:
                console.print(f"No items to download for {selector!s}. Skipping.")
                continue
            total_item_count = total if (limit is None or total is None) else min(limit, total)

            filestem = sanitize_filename(str(selector))
            start_item = 0
            init_bookmark: Bookmark | None = None
            # Remove false when ready.
            if False and Flags.EXTEND_DOWNLOAD.is_enabled():
                if progress := ProgressYAML.try_load(target_dir, self._download_filestem(filestem)):
                    if progress.total != total_item_count:
                        console.print(
                            f"Found progress file for {selector.display_name}. But total items "
                            f"does not match the expected total. Starting from beginning..."
                        )
                    elif progress.status == "completed":
                        console.print(f"Found completed progress file for {selector.display_name}. Skipping download.")
                    elif first := progress.get_first_bookmark():
                        init_bookmark = first
                        start_item = progress.completed_count
                        console.print(f"Resuming download for {selector.display_name} from {first!s}.")
                    else:
                        console.print(
                            f"Found progress file but failed to load for {selector.display_name}. "
                            f"Starting from beginning"
                        )
            else:
                if self._already_downloaded(target_dir, filestem):
                    warning = LowSeverityWarning(
                        f"Data for {selector!s} already exists in {target_dir.as_posix()!r}. Skipping download."
                    )
                    self.warn(warning, console=console)
                    continue

            selector.dump_to_file(target_dir)
            columns: list[SchemaColumn] | None = None
            is_table = file_format in TABLE_WRITE_CLS_BY_FORMAT
            if is_table and isinstance(io, TableStorageIO):
                columns = io.get_schema(selector)
            elif is_table:
                raise ToolkitValueError(
                    f"Cannot download {selector.kind} in {file_format!r} format. The {selector.kind!r} storage type does not support table schemas."
                )

            with FileWriter.create_from_format(
                file_format, target_dir, selector.kind, compression_cls, columns=columns
            ) as writer:
                executor = ProducerWorkerExecutor[Page[T_ResourceResponse], Page[dict[str, JsonVal]]](
                    download_iterable=io.stream_data(selector, limit, bookmark=init_bookmark),
                    process=self.create_data_process(io=io, selector=selector, is_table=is_table),
                    write=self.create_writer(writer, filestem, target_dir, start_item, total_item_count),
                    total_item_count=total_item_count,
                    # Limit queue size to avoid filling up memory before the workers can write to disk.
                    max_queue_size=8 * 10,  # 8 workers, 10 items per worker
                    download_description=f"Downloading {selector!s}",
                    process_description="Processing",
                    write_description=f"Writing to {target_dir.as_posix()!r} in files with stem {filestem!r}",
                    console=console,
                )
                executor.run(start_item=start_item)
                progress = ProgressYAML.try_load(target_dir, filestem=self._download_filestem(filestem))
                if progress is not None:
                    progress.status = executor.result
                    progress.dump_to_file(target_dir, filestem=self._download_filestem(filestem))

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
    def _download_filestem(filestem: str) -> str:
        return f"downloaded_{filestem!s}"

    @staticmethod
    def create_data_process(
        io: StorageIO[T_Selector, T_ResourceResponse],
        selector: T_Selector,
        is_table: bool,
    ) -> Callable[[Page[T_ResourceResponse]], Page[dict[str, JsonVal]]]:
        """Creates a data processing function based on the IO type and whether the output is a table."""
        if is_table and isinstance(io, TableStorageIO):
            return partial(io.data_to_row, selector=selector)
        return partial(io.data_to_json_chunk, selector=selector)

    @classmethod
    def create_writer(
        cls,
        writer: FileWriter,
        filestem: str,
        directory: Path,
        start_item: int,
        total_item_count: int | None,
    ) -> Callable[[Page[dict[str, JsonVal]]], None]:
        """Creates a writer function that writes processed data to files using the provided FileWriter."""
        write_item_count = start_item

        def write(page: Page[dict[str, JsonVal]]) -> None:
            # MyPy Fails to understand that JsonVal is a subset of chunk.
            nonlocal write_item_count
            writer.write_chunks(page.as_raw_items(), filestem=filestem)  # type: ignore[arg-type]

            # Remove false when functionality is ready.
            if False and Flags.EXTEND_DOWNLOAD.is_enabled():
                write_item_count += len(page.as_raw_items())
                ProgressYAML(
                    status="in-progress",
                    bookmarks={page.worker_id: page.bookmark},
                    total=total_item_count,
                    completed_count=write_item_count,
                ).dump_to_file(
                    directory=directory,
                    filestem=cls._download_filestem(filestem),
                )

        return write
