from collections.abc import Callable, Iterable, Sequence
from enum import Enum
from pathlib import Path

from rich.console import Console
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import DataMapper
from cognite_toolkit._cdf_tk.constants import DMS_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitMigrationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.storageio import StorageIO
from cognite_toolkit._cdf_tk.storageio._base import T_CogniteResourceList, T_Selector, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import Chunk, CSVWriter, NDJsonWriter, SchemaColumn, Uncompressed
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemIDMessage, SuccessItem
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.progress_tracker import AVAILABLE_STATUS, ProgressTracker, Status
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID

from .data_model import INSTANCE_SOURCE_VIEW_ID, MODEL_ID, RESOURCE_VIEW_MAPPING_VIEW_ID


class MigrationCommand(ToolkitCommand):
    class Steps(str, Enum):
        DOWNLOAD = "download"
        CONVERT = "convert"
        UPLOAD = "upload"

        @classmethod
        def list(cls) -> list[str]:
            return [step.value for step in cls.__members__.values()]

    def migrate(
        self,
        selected: T_Selector,
        data: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        mapper: DataMapper[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceList],
        log_dir: Path,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> ProgressTracker[T_ID]:
        if log_dir.exists():
            raise ToolkitFileExistsError(
                f"Log directory {log_dir} already exists. Please remove it or choose another directory."
            )
        self.validate_migration_model_available(data.client)
        log_dir.mkdir(parents=True, exist_ok=False)
        mapper.prepare(selected)

        iteration_count: int | None = None
        total_items = data.count(selected)
        if total_items is not None:
            iteration_count = (total_items // data.CHUNK_SIZE) + (1 if total_items % data.CHUNK_SIZE > 0 else 0)
            self.validate_available_capacity(data.client, total_items)

        console = Console()
        tracker = ProgressTracker[T_ID](self.Steps.list())
        with (
            NDJsonWriter(log_dir, kind=f"{data.KIND}MigrationIssues", compression=Uncompressed) as log_file,
            HTTPClient(config=data.client.config) as write_client,
        ):
            executor = ProducerWorkerExecutor[T_WritableCogniteResourceList, T_CogniteResourceList](
                download_iterable=self._download_iterable(selected, data, tracker),
                process=self._convert(mapper, data, tracker, log_file),
                write=self._upload(write_client, data, tracker, log_file, dry_run),
                iteration_count=iteration_count,
                max_queue_size=10,
                download_description=f"Downloading {data.DISPLAY_NAME}",
                process_description="Converting",
                write_description="Uploading",
                console=console,
            )

            executor.run()
            total = executor.total_items

        self._print_table(tracker.aggregate(), console)
        self._print_csv(tracker, log_dir, f"{data.KIND}Items", console)
        executor.raise_on_error()
        action = "Would migrate" if dry_run else "Migrating"
        console.print(f"{action} {total:,} {data.DISPLAY_NAME} to instances.")
        return tracker

    def _print_table(self, results: dict[tuple[str, Status], int], console: Console) -> None:
        for step in self.Steps:
            # We treat pending as failed for summary purposes
            results[(step.value, "failed")] = results.get((step.value, "failed"), 0) + results.get(
                (step.value, "pending"), 0
            )

        table = Table(title="Migration Summary", show_lines=True)
        table.add_column("Status", style="cyan", no_wrap=True)
        for step in self.Steps:
            table.add_column(step.value.capitalize(), style="magenta")
        for status in AVAILABLE_STATUS:
            if status == "pending":
                # Skip pending as we treat it as failed
                continue
            row = [status]
            for step in self.Steps:
                row.append(str(results.get((step.value, status), 0)))
            table.add_row(*row)

        console.print(table)

    def _print_csv(self, tracker: ProgressTracker[T_ID], log_dir: Path, kind: str, console: Console) -> None:
        with CSVWriter(log_dir, kind=kind, compression=Uncompressed, columns=self._csv_columns()) as csv_file:
            batch: list[Chunk] = []
            steps = self.Steps.list()
            for item_id, progress in tracker.result().items():
                batch.append({"ID": str(item_id), **{step: progress[step] for step in steps}})
                if len(batch) >= 1000:
                    csv_file.write_chunks(batch)
                    batch = []
            if batch:
                csv_file.write_chunks(batch)
        console.print(f"Migration items written to {log_dir}")

    @classmethod
    def _csv_columns(cls) -> list[SchemaColumn]:
        return [
            SchemaColumn(name="ID", type="string"),
            *(SchemaColumn(name=step, type="string") for step in cls.Steps.list()),
        ]

    def _download_iterable(
        self,
        selected: T_Selector,
        data: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        tracker: ProgressTracker[T_ID],
    ) -> Iterable[T_WritableCogniteResourceList]:
        for chunk in data.stream_data(selected):
            for item in chunk:
                tracker.set_progress(data.as_id(item), self.Steps.DOWNLOAD, "success")
            yield chunk

    def _convert(
        self,
        mapper: DataMapper[T_Selector, T_WritableCogniteResourceList, T_CogniteResourceList],
        data: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        tracker: ProgressTracker[T_ID],
        log_file: NDJsonWriter,
    ) -> Callable[[T_WritableCogniteResourceList], T_CogniteResourceList]:
        def track_mapping(source: T_WritableCogniteResourceList) -> T_CogniteResourceList:
            target, issues = mapper.map_chunk(source)
            for item in source:
                tracker.set_progress(data.as_id(item), step=self.Steps.CONVERT, status="success")
            if issues:
                # MyPy fails to understand that dict[str, JsonVal] is a Chunk
                log_file.write_chunks([issue.dump() for issue in issues])  # type: ignore[misc]
            return target

        return track_mapping

    def _upload(
        self,
        write_client: HTTPClient,
        target: StorageIO[T_ID, T_Selector, T_CogniteResourceList, T_WritableCogniteResourceList],
        tracker: ProgressTracker[T_ID],
        log_file: NDJsonWriter,
        dry_run: bool,
    ) -> Callable[[T_CogniteResourceList], None]:
        def upload_items(data_chunk: T_CogniteResourceList) -> None:
            if not data_chunk:
                return None
            results: Sequence[HTTPMessage]
            if dry_run:
                results = [SuccessItem(200, target.as_id(item)) for item in data_chunk]
            else:
                results = target.upload_items(data_chunk=data_chunk, http_client=write_client, selector=None)

            issues: list[Chunk] = []
            for item in results:
                if isinstance(item, SuccessItem):
                    tracker.set_progress(item.id, step=self.Steps.UPLOAD, status="success")
                elif isinstance(item, ItemIDMessage):
                    tracker.set_progress(item.id, step=self.Steps.UPLOAD, status="failed")
                if not isinstance(item, SuccessItem):
                    # MyPy fails to understand that dict[str, JsonVal] is a Chunk
                    issues.append(item.dump())  # type: ignore[arg-type]
            if issues:
                log_file.write_chunks(issues)
            return None

        return upload_items

    @staticmethod
    def validate_migration_model_available(client: ToolkitClient) -> None:
        models = client.data_modeling.data_models.retrieve([MODEL_ID], inline_views=False)
        if not models:
            raise ToolkitMigrationError(
                f"The migration data model {MODEL_ID!r} does not exist. "
                "Please run the `cdf migrate prepare` command to deploy the migration data model."
            )
        elif len(models) > 1:
            raise ToolkitMigrationError(
                f"Multiple migration models {MODEL_ID!r}. "
                "Please delete the duplicate models before proceeding with the migration."
            )
        model = models[0]
        missing_views = {INSTANCE_SOURCE_VIEW_ID, RESOURCE_VIEW_MAPPING_VIEW_ID} - set(model.views or [])
        if missing_views:
            raise ToolkitMigrationError(
                f"Invalid migration model. Missing views {humanize_collection(missing_views)}. "
                f"Please run the `cdf migrate prepare` command to deploy the migration data model."
            )

    def validate_available_capacity(self, client: ToolkitClient, instance_count: int) -> None:
        """Validate that the project has enough capacity to accommodate the migration."""

        stats = client.data_modeling.statistics.project()

        available_capacity = stats.instances.instances_limit - stats.instances.instances
        available_capacity_after = available_capacity - instance_count

        if available_capacity_after < DMS_INSTANCE_LIMIT_MARGIN:
            raise ToolkitValueError(
                "Cannot proceed with migration, not enough instance capacity available. Total capacity after migration"
                f" would be {available_capacity_after:,} instances, which is less than the required margin of"
                f" {DMS_INSTANCE_LIMIT_MARGIN:,} instances. Please increase the instance capacity in your CDF project"
                f" or delete some existing instances before proceeding with the migration of {instance_count:,} assets."
            )
        total_instances = stats.instances.instances + instance_count
        self.console(
            f"Project has enough capacity for migration. Total instances after migration: {total_instances:,}."
        )
