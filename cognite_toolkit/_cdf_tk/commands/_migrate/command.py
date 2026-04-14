import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Generic

from rich import print
from rich.console import Console
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsFailedRequest,
    ItemsFailedResponse,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import MigrationCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import DataMapper
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import RecordsMigrationIO
from cognite_toolkit._cdf_tk.commands.deploy import DeployCommand
from cognite_toolkit._cdf_tk.constants import DMS_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.data_classes import DeployResults
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitMigrationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.resource_ios import ResourceWorker
from cognite_toolkit._cdf_tk.storageio import (
    ChartIO,
    DataItem,
    Page,
    T_DataRequest,
    T_DataResponse,
    T_Selector,
    UploadableStorageIO,
)
from cognite_toolkit._cdf_tk.storageio.logger import (
    FileWithAggregationLogger,
    ItemsResult,
    display_item_results,
)
from cognite_toolkit._cdf_tk.storageio.progress import Bookmark, CursorBookmark, ProgressYAML
from cognite_toolkit._cdf_tk.utils import humanize_collection, safe_write, sanitize_filename
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter, Uncompressed
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor

from .data_model import INSTANCE_SOURCE_VIEW_ID, MODEL_ID, RESOURCE_VIEW_MAPPING_VIEW_ID
from .issues import WriteIssue, write_issue_as_migration_entry


@dataclass
class MigrationStep(Generic[T_Selector]):
    total_count: int | None
    completed_count: int
    bookmark: Bookmark | None
    message: str
    is_completed: bool
    selector: T_Selector


class MigrationCommand(ToolkitCommand):
    def migrate(
        self,
        selectors: Sequence[T_Selector],
        data: UploadableStorageIO[T_Selector, T_DataResponse, T_DataRequest],
        mapper: DataMapper[T_Selector, T_DataResponse, T_DataRequest],
        log_dir: Path,
        dry_run: bool = False,
        verbose: bool = False,
        user_log_filestem: str | None = None,
    ) -> dict[str, list[ItemsResult]]:
        self.validate_migration_model_available(data.client)
        log_dir.mkdir(parents=True, exist_ok=True)
        log_filestem = self._create_logfile_stem(log_dir, user_log_filestem, data.KIND)

        console = data.client.console

        plan = self._create_plan(data, selectors, log_dir)
        self._display_plan(plan, console)

        needed_capacity = sum(
            (step.total_count - step.completed_count) for step in plan if step.total_count is not None
        )

        if needed_capacity and not isinstance(data, ChartIO):
            # Charts are not creating any new nodes.
            if isinstance(data, RecordsMigrationIO):
                self.validate_stream_capacity(data.client, data.stream_external_id, needed_capacity)
            else:
                self.validate_available_capacity(data.client, needed_capacity)
        results_by_selector: dict[str, list[ItemsResult]] = {}
        with (
            NDJsonWriter(
                log_dir, kind="MigrationIssues", default_filestem=log_filestem, compression=Uncompressed
            ) as log_file,
            HTTPClient(config=data.client.config) as write_client,
        ):
            with FileWithAggregationLogger(log_file) as logger:
                self._run_migration_steps(
                    plan=plan,
                    data=data,
                    mapper=mapper,
                    logger=logger,
                    write_client=write_client,
                    log_dir=log_dir,
                    dry_run=dry_run,
                    verbose=verbose,
                    console=console,
                    results_by_selector=results_by_selector,
                )

        return results_by_selector

    def _run_migration_steps(
        self,
        plan: Sequence[MigrationStep],
        data: UploadableStorageIO[T_Selector, T_DataResponse, T_DataRequest],
        mapper: DataMapper[T_Selector, T_DataResponse, T_DataRequest],
        logger: FileWithAggregationLogger,
        write_client: HTTPClient,
        log_dir: Path,
        dry_run: bool,
        verbose: bool,
        console: Console,
        results_by_selector: dict[str, list[ItemsResult]],
    ) -> None:
        data.logger = logger
        mapper.logger = logger
        for step in plan:
            if step.message:
                console.print(step.message)
            if step.is_completed:
                continue

            selected = step.selector
            logger.reset()
            mapper.prepare(selected)

            executor = ProducerWorkerExecutor[Page[T_DataResponse], Page[T_DataRequest]](
                download_iterable=data.stream_data(selected, bookmark=step.bookmark),
                process=self._convert(mapper),
                write=self._upload(
                    selected, write_client, data, dry_run, log_dir, step.total_count, step.completed_count
                ),
                total_item_count=step.total_count,
                max_queue_size=10,
                download_description=f"Downloading {selected.display_name}",
                process_description="Converting",
                write_description="Uploading",
                console=console,
                verbose=verbose,
            )

            executor.run(start_item=step.completed_count)
            total = executor.downloaded_items

            items_results = logger.finalize(dry_run)
            results_by_selector[str(selected)] = items_results

            display_item_results(items_results, console)
            self._print_txt(items_results, log_dir, f"{selected!s}Items", console)
            progress = ProgressYAML.try_load(log_dir, filestem=str(selected))
            if progress is not None:
                progress.status = executor.result
                progress.dump_to_file(log_dir, filestem=str(selected))
            executor.raise_on_error()

            action = "Would migrate" if dry_run else "Migrating"
            target = "records" if isinstance(data, RecordsMigrationIO) else "instances"
            console.print(f"{action} {total:,} {selected.display_name} to {target}.")

    def _create_plan(
        self,
        data: UploadableStorageIO[T_Selector, T_DataResponse, T_DataRequest],
        selectors: Sequence[T_Selector],
        log_dir: Path,
    ) -> list[MigrationStep]:
        plan: list[MigrationStep] = []
        for selector in selectors:
            total_items = data.count(selector)
            completed_count = 0
            init_bookmark: Bookmark | None = None
            message = ""

            is_complete = False
            if progress := ProgressYAML.try_load(log_dir, str(selector)):
                completed_count = progress.completed_count
                first = progress.get_first_bookmark()
                is_sync = isinstance(first, CursorBookmark) and first.source == "sync"
                # Sync cursor supports continuing even if the data has been modified.
                if progress.total != total_items and not is_sync:
                    message = (
                        f"Found progress file for {selector.display_name}. But total items "
                        f"does not match the expected total. Starting from beginning..."
                    )
                elif progress.status == "completed" and (not is_sync or completed_count == total_items):
                    message = f"Found completed progress file for {selector.display_name}. Skipping migration."
                    is_complete = True
                elif first is not None:
                    init_bookmark = first
                    message = f"Resuming migration for {selector.display_name} from {first!s}."
                else:
                    message = (
                        f"Found progress file but failed to load for {selector.display_name}. Starting from beginning"
                    )
            plan.append(
                MigrationStep(
                    total_count=total_items,
                    completed_count=completed_count,
                    bookmark=init_bookmark,
                    message=message,
                    is_completed=is_complete,
                    selector=selector,
                )
            )
        return plan

    def _display_plan(
        self,
        plan: Sequence[MigrationStep],
        console: Console,
    ) -> None:
        table = Table(title="Planned Migrations")
        table.add_column("Data Type", style="cyan")
        table.add_column("Completed Count", style="cyan")
        table.add_column("Total Count", justify="right", style="green")
        total_count = 0
        total_completed = 0
        for step in plan:
            total_count += step.total_count if step.total_count is not None else 0
            total_completed += step.completed_count if step.completed_count is not None else 0
            item_count = f"{step.total_count:,}" if step.total_count is not None else "Unknown"
            table.add_row(str(step.selector), f"{step.completed_count:,}", item_count)

        table.add_section()
        table.add_row("Total", f"{total_completed:,}", f"{total_count:,}")
        console.print(table)
        return None

    @staticmethod
    def _create_logfile_stem(log_dir: Path, user_log_filestem: str | None, data_kind: str) -> str:
        """Create a filestem for the log file that does not conflict with existing files in the log directory."""
        base_logstem = user_log_filestem or data_kind
        if not base_logstem.endswith("-"):
            base_logstem += "-"

        existing_files = list(log_dir.glob(f"{base_logstem}*"))
        if not existing_files:
            return base_logstem

        run_pattern = re.compile(re.escape(base_logstem) + r"run(\d+)-")
        max_run = 0
        for f in existing_files:
            match = run_pattern.match(f.name)
            if match:
                max_run = max(max_run, int(match.group(1)))

        # If max_run is 0, it means files with base_logstem exist, but none have 'runX'.
        next_run = max(2, max_run + 1)

        return f"{base_logstem}run{next_run}-"

    def _print_txt(self, results: list[ItemsResult], log_dir: Path, filestem: str, console: Console) -> None:
        summary_file = log_dir / f"{filestem}_migration_summary.txt"
        with summary_file.open("w", encoding="utf-8") as f:
            f.write("Migration Summary\n")
            f.write("=================\n\n")
            for result in results:
                f.write(f"Status: {result.status}\n")
                f.write(f"Count: {result.count}\n")
                f.write("Labels:\n")
                if result.labels:
                    for label in result.labels:
                        f.write(f"  - {label.display_message()}\n")
                else:
                    f.write("  None\n")
                f.write("\n")
        console.print(f"Summary written to {log_dir}")

    @staticmethod
    def _convert(
        mapper: DataMapper[T_Selector, T_DataResponse, T_DataRequest],
    ) -> Callable[[Page[T_DataResponse]], Page[T_DataRequest]]:
        def track_mapping(source: Page[T_DataResponse]) -> Page[T_DataRequest]:
            raw_items = [di.item for di in source.items]
            mapped = mapper.map(raw_items)
            return Page(
                worker_id=source.worker_id,
                items=[
                    DataItem(tracking_id=item.tracking_id, item=target)
                    for target, item in zip(mapped, source.items)
                    if target is not None
                ],
                bookmark=source.bookmark,
            )

        return track_mapping

    def _upload(
        self,
        selected: T_Selector,
        write_client: HTTPClient,
        target: UploadableStorageIO[T_Selector, T_DataResponse, T_DataRequest],
        dry_run: bool,
        log_dir: Path,
        total_item_count: int | None,
        start_item: int,
    ) -> Callable[[Page[T_DataRequest]], None]:
        migrate_count: int = start_item

        def upload_items(page: Page[T_DataRequest]) -> None:
            nonlocal migrate_count
            if not page:
                return None
            if dry_run:
                return None

            responses = target.upload_items(data_chunk=page, http_client=write_client, selector=selected)

            issues: list[WriteIssue] = []
            for item in responses:
                if isinstance(item, ItemsSuccessResponse):
                    continue
                if isinstance(item, ItemsFailedResponse):
                    error = item.error
                    for id_ in item.ids:
                        issue = WriteIssue(id=str(id_), status_code=error.code, message=error.message)
                        issues.append(issue)
                elif isinstance(item, ItemsFailedRequest):
                    for id_ in item.ids:
                        issue = WriteIssue(id=str(id_), status_code=0, message=item.error_message)
                        issues.append(issue)

            if issues:
                destination = target.KIND
                source = str(selected)
                target.logger.log(
                    [write_issue_as_migration_entry(issue, source=source, destination=destination) for issue in issues]
                )

            migrate_count += sum(len(response.ids) for response in responses)
            ProgressYAML(
                status="in-progress",
                bookmarks={page.worker_id: page.bookmark},
                total=total_item_count,
                completed_count=migrate_count,
            ).dump_to_file(log_dir, filestem=str(selected))
            return None

        return upload_items

    def validate_stream_capacity(self, client: ToolkitClient, stream_external_id: str, record_count: int) -> None:
        results = client.streams.retrieve(
            [ExternalId(external_id=stream_external_id)], include_statistics=True, ignore_unknown_ids=True
        )
        if not results:
            raise ToolkitMigrationError(
                f"Stream '{stream_external_id}' does not exist. "
                "Please create the stream before running a records migration."
            )
        stream = results[0]
        limits = stream.settings.limits if stream.settings else None
        if limits is None:
            self.console(f"Unable to check stream capacity for '{stream.external_id}' (no settings returned).")
            return

        records_usage = limits.max_records_total
        records_consumed = records_usage.consumed or 0
        records_available = records_usage.provisioned - records_consumed

        if records_available < record_count:
            raise ToolkitValueError(
                f"Stream '{stream.external_id}' does not have enough record capacity. "
                f"Provisioned: {records_usage.provisioned:,}, consumed: {records_consumed:,}, "
                f"available: {records_available:,}, needed: {record_count:,}."
            )

        storage_usage = limits.max_giga_bytes_total
        storage_consumed = storage_usage.consumed or 0
        storage_available = storage_usage.provisioned - storage_consumed
        if storage_available <= 0:
            raise ToolkitValueError(
                f"Stream '{stream.external_id}' does not have enough storage capacity. "
                f"Provisioned: {storage_usage.provisioned:,} GB, consumed: {storage_consumed:,} GB."
            )

        records_total_after = records_consumed + record_count
        self.console(
            f"Stream '{stream.external_id}' has enough capacity. "
            f"Records after migration: {records_total_after:,} / {records_usage.provisioned:,}. "
        )
        self.console(
            f"Before migration, you've so far used {storage_consumed:,} / {storage_usage.provisioned:,} GB of stream storage. "
            "Note that storage capacity is NOT considered when checking for capacity, and the migration might fail if you end up going over this limit."
        )

    @staticmethod
    def validate_migration_model_available(client: ToolkitClient) -> None:
        models = client.tool.data_models.retrieve([MODEL_ID], inline_views=False)
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
        available_capacity_after = available_capacity - max(instance_count, 0)

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

    def create(
        self,
        client: ToolkitClient,
        creator: MigrationCreator,
        dry_run: bool,
        output_dir: Path,
        deploy: bool = True,
        verbose: bool = False,
    ) -> DeployResults:
        """This method is used to create migration resource in CDF."""
        self.validate_migration_model_available(client)

        deploy_cmd = DeployCommand(self.print_warning, silent=self.silent)
        deploy_cmd.tracker = self.tracker
        # crud_cls = creator.CRUD
        # resource_list = creator.create_resources()
        results = DeployResults([], "deploy", dry_run=dry_run)
        for to_create in creator.create_resources():
            crud_cls = to_create.crud_cls
            if deploy:
                crud = crud_cls.create_loader(client)
                worker = ResourceWorker(crud, "deploy")
                local_by_id = {
                    crud.get_id(item.resource): (item.resource.dump(), item.resource) for item in to_create.resources
                }
                worker.validate_access(local_by_id, is_dry_run=dry_run)
                cdf_resources = crud.retrieve(list(local_by_id.keys()))
                resources = worker.categorize_resources(local_by_id, cdf_resources, False, verbose)

                if dry_run:
                    result = deploy_cmd.dry_run_deploy(resources, crud, False, False)
                else:
                    result = deploy_cmd.actual_deploy(resources, crud)
                    if result.calculated_total > 0 and to_create.store_linage is not None:
                        store_count = to_create.store_linage()
                        self.console(f"Stored lineage for {store_count:,} {to_create.display_name}.")

                verb = "Would deploy" if dry_run else "Deploying"
                self.console(f"{verb} {to_create.display_name} to CDF.")

                if result:
                    results[result.name] = result

                if results.has_counts:
                    print(results.counts_table())

            for item in to_create.resources:
                if item.config_data and item.filestem:
                    filepath = (
                        output_dir / crud_cls.folder_name / f"{sanitize_filename(item.filestem)}.{crud_cls.kind}.yaml"
                    )
                    filepath.parent.mkdir(parents=True, exist_ok=True)
                    safe_write(filepath, yaml_safe_dump(item.config_data))
            self.console(
                f"{len(to_create.resources)} {crud_cls.kind} resource configurations written to {(output_dir / crud_cls.folder_name).as_posix()!r}"
            )
            self.console("It is recommended to add these files to a Toolkit governed module.")

        return results
