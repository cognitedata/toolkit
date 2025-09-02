from functools import partial
from pathlib import Path

from rich.console import Console

from cognite_toolkit._cdf_tk.exceptions import ToolkitFileExistsError
from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.storageio._base import T_CogniteResourceList, T_Selector, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils.fileio import CSVWriter, NDJsonWriter, Uncompressed
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor


class ConversionIO: ...


class MigrationCommand:
    def migrate(
        self,
        selected: T_Selector,
        source: StorageIO[T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        conversion: ConversionIO,
        target: StorageIO,
        log_dir: Path,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        if log_dir.exists():
            raise ToolkitFileExistsError(
                f"Log directory {log_dir} already exists. Please remove it or choose another directory."
            )
        log_dir.mkdir(parents=True, exist_ok=False)
        source.validate_access("READ")
        target.validate_access("WRITE")
        conversion.prepare(selected)

        iteration_count: int | None = None
        total_items = source.count(selected)
        if total_items is not None:
            iteration_count = (total_items // source.chunk_size) + (1 if total_items % source.chunk_size > 0 else 0)

        console = Console()
        # Todo Replace with thread safe structure.
        overview = dict()
        with (
            CSVWriter(
                log_dir,
                kind=f"{source.kind}Migration",
                compression=Uncompressed,
                columns=self._migration_result_schema(),
            ) as result_file,
            NDJsonWriter(
                log_dir,
                kind=f"{source.kind}MigrationIssues",
                compression=Uncompressed,
            ) as log_file,
        ):
            executor = ProducerWorkerExecutor(
                download_iterable=source.download_iterable(selected, overview),
                process=partial(conversion.convert_chunk, log_file=log_file, verbose=verbose, overview=overview),
                write=partial(target.upload_items, log_file, dry_run=dry_run, verbose=verbose, overview=overview),
                iteration_count=iteration_count,
                max_queue_size=10,
                download_description=f"Downloading {source.display_name}",
                process_description=f"Converting {source.display_name} to {target.display_name}",
                write_description=f"Uploading to {target.display_name}",
                console=console,
            )

            executor.run()
            total = executor.total_items

        executor.raise_on_error()
        self.print_table(overview, console=console)
        action = "Would migrate" if dry_run else "Migrating"
        console.print(f"{action} {total:,} {source.display_name} to {target.display_name}.")
