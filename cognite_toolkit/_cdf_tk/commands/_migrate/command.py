from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import get_args

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
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands._migrate.creators import MigrationCreator
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import DataMapper
from cognite_toolkit._cdf_tk.commands.deploy import DeployCommand
from cognite_toolkit._cdf_tk.constants import DMS_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.cruds import ResourceWorker
from cognite_toolkit._cdf_tk.data_classes import DeployResults
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitFileExistsError,
    ToolkitMigrationError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.protocols import T_ResourceRequest, T_ResourceResponse
from cognite_toolkit._cdf_tk.storageio import T_Selector, UploadableStorageIO, UploadItem
from cognite_toolkit._cdf_tk.storageio.logger import FileDataLogger, OperationStatus
from cognite_toolkit._cdf_tk.utils import humanize_collection, safe_write, sanitize_filename
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter, Uncompressed
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor

from .data_model import INSTANCE_SOURCE_VIEW_ID, MODEL_ID, RESOURCE_VIEW_MAPPING_VIEW_ID
from .issues import WriteIssue


@dataclass
class OperationIssue:
    message: str
    count: int


@dataclass
class MigrationStatusResult:
    status: OperationStatus
    issues: list[OperationIssue]
    count: int


class MigrationCommand(ToolkitCommand):
    def migrate(
        self,
        selected: T_Selector,
        data: UploadableStorageIO[T_Selector, T_ResourceResponse, T_ResourceRequest],
        mapper: DataMapper[T_Selector, T_ResourceResponse, T_ResourceRequest],
        log_dir: Path,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> list[MigrationStatusResult]:
        if log_dir.exists() and any(log_dir.iterdir()):
            raise ToolkitFileExistsError(
                f"Log directory {log_dir} already exists. Please remove it or choose another directory."
            )
        self.validate_migration_model_available(data.client)
        log_dir.mkdir(parents=True, exist_ok=True)
        mapper.prepare(selected)

        iteration_count: int | None = None
        total_items = data.count(selected)
        if total_items is not None:
            iteration_count = (total_items // data.CHUNK_SIZE) + (1 if total_items % data.CHUNK_SIZE > 0 else 0)
            self.validate_available_capacity(data.client, total_items)

        console = Console()
        with (
            NDJsonWriter(log_dir, kind=f"{selected.kind}MigrationIssues", compression=Uncompressed) as log_file,
            HTTPClient(config=data.client.config) as write_client,
        ):
            logger = FileDataLogger(log_file)
            data.logger = logger
            mapper.logger = logger

            executor = ProducerWorkerExecutor[Sequence[T_ResourceResponse], Sequence[UploadItem[T_ResourceRequest]]](
                download_iterable=(page.items for page in data.stream_data(selected)),
                process=self._convert(mapper, data),
                write=self._upload(selected, write_client, data, dry_run),
                iteration_count=iteration_count,
                max_queue_size=10,
                download_description=f"Downloading {selected.display_name}",
                process_description="Converting",
                write_description="Uploading",
                console=console,
                verbose=verbose,
            )

            executor.run()
            total = executor.total_items

        results = self._create_status_summary(logger)

        self._print_rich_tables(results, console)
        self._print_txt(results, log_dir, f"{selected.kind}Items", console)
        executor.raise_on_error()
        action = "Would migrate" if dry_run else "Migrating"
        console.print(f"{action} {total:,} {selected.display_name} to instances.")

        return results

    # Todo: Move to the logger module
    @classmethod
    def _create_status_summary(cls, logger: FileDataLogger) -> list[MigrationStatusResult]:
        results: list[MigrationStatusResult] = []
        status_counts = logger.tracker.get_status_counts()
        for status in get_args(OperationStatus):
            issue_counts = logger.tracker.get_issue_counts(status)
            issues = [OperationIssue(message=issue, count=count) for issue, count in issue_counts.items()]
            result = MigrationStatusResult(
                status=status,
                issues=issues,
                count=status_counts.get(status, 0),
            )
            results.append(result)
        return results

    def _print_rich_tables(self, results: list[MigrationStatusResult], console: Console) -> None:
        table = Table(title="Migration Summary", show_lines=True)
        table.add_column("Status", style="bold")
        table.add_column("Count", justify="right", style="bold")
        table.add_column("Issues", style="bold")
        for result in results:
            issues_str = "\n".join(f"{issue.message}: {issue.count}" for issue in result.issues) or ""
            table.add_row(result.status, str(result.count), issues_str)
        console.print(table)

    def _print_txt(self, results: list[MigrationStatusResult], log_dir: Path, kind: str, console: Console) -> None:
        summary_file = log_dir / f"{kind}_migration_summary.txt"
        with summary_file.open("w", encoding="utf-8") as f:
            f.write("Migration Summary\n")
            f.write("=================\n\n")
            for result in results:
                f.write(f"Status: {result.status}\n")
                f.write(f"Count: {result.count}\n")
                f.write("Issues:\n")
                if result.issues:
                    for issue in result.issues:
                        f.write(f"  - {issue.message}: {issue.count}\n")
                else:
                    f.write("  None\n")
                f.write("\n")
        console.print(f"Summary written to {log_dir}")

    @staticmethod
    def _convert(
        mapper: DataMapper[T_Selector, T_ResourceResponse, T_ResourceRequest],
        data: UploadableStorageIO[T_Selector, T_ResourceResponse, T_ResourceRequest],
    ) -> Callable[[Sequence[T_ResourceResponse]], Sequence[UploadItem[T_ResourceRequest]]]:
        def track_mapping(source: Sequence[T_ResourceResponse]) -> list[UploadItem[T_ResourceRequest]]:
            mapped = mapper.map(source)
            return [
                UploadItem(source_id=data.as_id(item), item=target)
                for target, item in zip(mapped, source)
                if target is not None
            ]

        return track_mapping

    def _upload(
        self,
        selected: T_Selector,
        write_client: HTTPClient,
        target: UploadableStorageIO[T_Selector, T_ResourceResponse, T_ResourceRequest],
        dry_run: bool,
    ) -> Callable[[Sequence[UploadItem[T_ResourceRequest]]], None]:
        def upload_items(data_item: Sequence[UploadItem[T_ResourceRequest]]) -> None:
            if not data_item:
                return None
            if dry_run:
                target.logger.tracker.finalize_item([item.source_id for item in data_item], "pending")
                return None

            responses = target.upload_items(data_chunk=data_item, http_client=write_client, selector=selected)

            # Todo: Move logging into the UploadableStorageIO class
            issues: list[WriteIssue] = []
            for item in responses:
                if isinstance(item, ItemsSuccessResponse):
                    target.logger.tracker.finalize_item(item.ids, "success")
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

                if isinstance(item, ItemsFailedResponse | ItemsFailedRequest):
                    target.logger.tracker.finalize_item(item.ids, "failure")
            if issues:
                target.logger.log(issues)
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

    def create(
        self,
        client: ToolkitClient,
        creator: MigrationCreator,
        dry_run: bool,
        output_dir: Path,
        verbose: bool = False,
    ) -> DeployResults:
        """This method is used to create migration resource in CDF."""
        self.validate_migration_model_available(client)

        deploy_cmd = DeployCommand(self.print_warning, silent=self.silent)
        deploy_cmd.tracker = self.tracker

        crud_cls = creator.CRUD
        resource_list = creator.create_resources()

        results = DeployResults([], "deploy", dry_run=dry_run)
        crud = crud_cls.create_loader(client)
        worker = ResourceWorker(crud, "deploy")
        local_by_id = {crud.get_id(item): (item.dump(), item) for item in resource_list}
        worker.validate_access(local_by_id, is_dry_run=dry_run)
        cdf_resources = crud.retrieve(list(local_by_id.keys()))
        resources = worker.categorize_resources(local_by_id, cdf_resources, False, verbose)

        if dry_run:
            result = deploy_cmd.dry_run_deploy(resources, crud, False, False)
        else:
            result = deploy_cmd.actual_deploy(resources, crud)
            if result.calculated_total > 0 and creator.HAS_LINEAGE:
                store_count = creator.store_lineage(resource_list)
                self.console(f"Stored lineage for {store_count:,} {creator.DISPLAY_NAME}.")

        verb = "Would deploy" if dry_run else "Deploying"
        self.console(f"{verb} {creator.DISPLAY_NAME} to CDF.")

        resource_configs = creator.resource_configs(resource_list)
        for config in resource_configs:
            filepath = output_dir / crud_cls.folder_name / f"{sanitize_filename(config.filestem)}.{crud_cls.kind}.yaml"
            filepath.parent.mkdir(parents=True, exist_ok=True)
            safe_write(filepath, yaml_safe_dump(config.data))
        self.console(
            f"{len(resource_configs)} {crud_cls.kind} resource configurations written to {(output_dir / crud_cls.folder_name).as_posix()!r}"
        )
        self.console("It is recommended to add these files to a Toolkit governed module.")

        if result:
            results[result.name] = result

        if results.has_counts:
            print(results.counts_table())

        return results
