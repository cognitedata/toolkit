import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from functools import partial
from pathlib import Path
from typing import Any, Literal, cast

import questionary
from cognite.client.data_classes import DataSetUpdate
from cognite.client.data_classes.data_modeling import Edge
from cognite.client.data_classes.data_modeling.statistics import SpaceStatistics
from pydantic import JsonValue
from rich import print
from rich.console import Console
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestItem
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsFailedRequest,
    ItemsFailedResponse,
    ItemsRequest,
    ItemsSuccessResponse,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.identifiers import (
    ContainerId,
    InstanceDefinitionId,
    InstanceId,
    InternalId,
    ViewId,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import ContainerFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId, SpaceId
from cognite_toolkit._cdf_tk.data_classes import DeployResults, ResourceDeployResult
from cognite_toolkit._cdf_tk.data_classes._tracking_info import DataTracking
from cognite_toolkit._cdf_tk.dataio import InstanceIO, Page
from cognite_toolkit._cdf_tk.dataio._base import DataItem
from cognite_toolkit._cdf_tk.dataio.logger import (
    FileWithAggregationLogger,
    ItemsResult,
    LogEntryV2,
    Severity,
    display_item_results,
)
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceSelector
from cognite_toolkit._cdf_tk.exceptions import (
    AuthorizationError,
    ToolkitMissingResourceError,
)
from cognite_toolkit._cdf_tk.protocols import ResourceResponseProtocol
from cognite_toolkit._cdf_tk.resource_ios import (
    AssetIO,
    ContainerCRUD,
    DataModelIO,
    EdgeCRUD,
    EventIO,
    ExtractionPipelineIO,
    FileMetadataCRUD,
    LabelIO,
    NodeCRUD,
    RelationshipIO,
    ResourceIO,
    SequenceIO,
    SpaceCRUD,
    ThreeDModelCRUD,
    TimeSeriesCRUD,
    TransformationIO,
    ViewIO,
    WorkflowIO,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    LimitedAccessWarning,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.aggregators import (
    AssetAggregator,
    EventAggregator,
    FileAggregator,
    LabelCountAggregator,
    RelationshipAggregator,
    SequenceAggregator,
    TimeSeriesAggregator,
)
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonWriter, Uncompressed
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
from cognite_toolkit._cdf_tk.utils.validate_access import ValidateAccess

from ._base import ToolkitCommand
from ._utils import (
    block_if_views_reference_containers,
    confirm_by_typing_project_name,
    print_soft_delete_panel,
    validate_soft_delete_headroom,
)


def _view_is_in_space(space: str, view: ViewId) -> bool:
    return view.space == space


@dataclass
class DeleteResults:
    deleted: int = 0
    failed: int = 0

    @classmethod
    def from_item_results(cls, items_results: list[ItemsResult]) -> "DeleteResults":
        deleted = 0
        failed = 0
        for result in items_results:
            if result.status in ("success", "success-with-warning", "pending", "pending-with-warning"):
                deleted += result.count
            elif result.status == "failure":
                failed += result.count
            # "skipped" items are neither deleted nor failed
        return cls(deleted=deleted, failed=failed)


class DeleteItem(RequestItem):
    item: JsonValue
    as_id_fun: Callable[[JsonValue], str]

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        return self.item  # type: ignore[return-value]

    def __str__(self) -> str:
        return self.as_id_fun(self.item)


@dataclass
class ToDelete(ABC):
    crud: ResourceIO
    total: int
    delete_url: str

    @property
    def display_name(self) -> str:
        return self.crud.display_name

    @abstractmethod
    def get_process_function(
        self, client: ToolkitClient, logger: FileWithAggregationLogger
    ) -> Callable[[Page[ResourceResponseProtocol]], Page[JsonVal]]:
        raise NotImplementedError()

    def get_extra_fields(self) -> dict[str, JsonVal]:
        return {}

    def get_tracking_id(self, resource: ResourceResponseProtocol) -> str:
        """Get a tracking ID for a resource. Override this for custom ID formats."""
        return str(self.crud.get_id(resource))


@dataclass
class DataModelingToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, logger: FileWithAggregationLogger
    ) -> Callable[[Page[ResourceResponseProtocol]], Page[JsonVal]]:
        def as_id(page: Page[ResourceResponseProtocol]) -> Page[JsonVal]:
            # We know that all data modeling resources implement as_id
            items = [
                DataItem(tracking_id=item.tracking_id, item=item.item.as_id().dump())  # type: ignore[attr-defined]
                for item in page.items
            ]
            return page.create_from(items)

        return as_id


@dataclass
class EdgeToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, logger: FileWithAggregationLogger
    ) -> Callable[[Page[ResourceResponseProtocol]], Page[JsonVal]]:
        def as_id(page: Page[ResourceResponseProtocol]) -> Page[JsonVal]:
            items: list[DataItem[JsonVal]] = [
                DataItem(
                    tracking_id=item.tracking_id,
                    item={
                        "space": cast(Edge, item.item).space,
                        "externalId": cast(Edge, item.item).external_id,
                        "instanceType": "edge",
                    },
                )
                for item in page.items
            ]
            return page.create_from(items)

        return as_id


@dataclass
class NodesToDelete(ToDelete):
    delete_datapoints: bool
    delete_file_content: bool

    def get_process_function(
        self, client: ToolkitClient, logger: FileWithAggregationLogger
    ) -> Callable[[Page[ResourceResponseProtocol]], Page[JsonVal]]:
        def check_for_data(page: Page[ResourceResponseProtocol]) -> Page[JsonVal]:
            tracking_by_node_id = {InstanceId(instance_id=item.item.as_id()): item.tracking_id for item in page.items}  # type: ignore[attr-defined]
            timeseries_ids: set[NodeId] = set()
            files_ids: set[NodeId] = set()
            if not self.delete_datapoints:
                timeseries = client.tool.timeseries.retrieve(list(tracking_by_node_id), ignore_unknown_ids=True)
                timeseries_ids = {ts.instance_id for ts in timeseries if ts.instance_id is not None}
            if not self.delete_file_content:
                files = client.tool.filemetadata.retrieve(list(tracking_by_node_id), ignore_unknown_ids=True)
                files_ids = {f.instance_id for f in files if f.instance_id is not None}
            for node_id in timeseries_ids:
                tracking_id = tracking_by_node_id.get(InstanceId(instance_id=node_id))
                if tracking_id:
                    logger.log(
                        LogEntryV2(
                            id=tracking_id,
                            severity=Severity.skipped,
                            label="Has datapoints",
                            message=f"Skipped node {node_id} as it has datapoints",
                        )
                    )
            for node_id in files_ids:
                tracking_id = tracking_by_node_id.get(InstanceId(instance_id=node_id))
                if tracking_id:
                    logger.log(
                        LogEntryV2(
                            id=tracking_id,
                            severity=Severity.skipped,
                            label="Has file content",
                            message=f"Skipped node {node_id} as it has file content",
                        )
                    )

            result: list[DataItem[JsonVal]] = []
            for item in page.items:
                node_id = item.item.as_id()  # type: ignore[attr-defined]
                if node_id not in timeseries_ids and node_id not in files_ids:
                    dumped = node_id.dump(include_instance_type=True)
                    result.append(DataItem(tracking_id=item.tracking_id, item=dumped))
            return page.create_from(result)

        return check_for_data


@dataclass
class IdResourceToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, logger: FileWithAggregationLogger
    ) -> Callable[[Page[ResourceResponseProtocol]], Page[JsonVal]]:
        def as_id(page: Page[ResourceResponseProtocol]) -> Page[JsonVal]:
            # We know that all id resources have an id attribute
            items: list[DataItem[JsonVal]] = [
                DataItem(tracking_id=item.tracking_id, item={"id": item.item.id})  # type: ignore[attr-defined]
                for item in page.items
            ]
            return page.create_from(items)

        return as_id

    def get_tracking_id(self, resource: ResourceResponseProtocol) -> str:
        return str(resource.id)  # type: ignore[attr-defined]


@dataclass
class ExternalIdToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, logger: FileWithAggregationLogger
    ) -> Callable[[Page[ResourceResponseProtocol]], Page[JsonVal]]:
        def as_external_id(page: Page[ResourceResponseProtocol]) -> Page[JsonVal]:
            # We know that all external id resources have an external_id attribute
            items: list[DataItem[JsonVal]] = [
                DataItem(tracking_id=item.tracking_id, item={"externalId": item.item.external_id})  # type: ignore[attr-defined]
                for item in page.items
            ]
            return page.create_from(items)

        return as_external_id

    def get_tracking_id(self, resource: ResourceResponseProtocol) -> str:
        return str(resource.external_id)  # type: ignore[attr-defined]


@dataclass
class AssetToDelete(IdResourceToDelete):
    recursive: bool

    def get_extra_fields(self) -> dict[str, JsonVal]:
        return {"recursive": self.recursive}


class PurgeCommand(ToolkitCommand):
    BATCH_SIZE_DM = 1000

    def space(
        self,
        client: ToolkitClient,
        selected_space: str,
        log_dir: Path,
        include_space: bool = False,
        delete_datapoints: bool = False,
        delete_file_content: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> DeployResults:
        stats = client.data_modeling.statistics.spaces.retrieve(selected_space)
        if stats is None:
            raise ToolkitMissingResourceError(f"Space {selected_space!r} does not exist")

        instance_count = stats.nodes + stats.edges

        validator = ValidateAccess(client, "purge")
        # TEMPORARY: The GET /models/statistics endpoint requires datamodelsAcl:read with All scope.
        # This check will be removed once DMS limits are available through the limits service.
        if instance_count > 0 and validator.data_model(["read"]) is not None:
            raise AuthorizationError(
                "Purging spaces containing instances currently requires datamodelsAcl:read with All scope."
            )

        if stats.containers > 0:
            container_ids = [
                ContainerId(space=c.space, external_id=c.external_id)
                for c in client.tool.containers.list(filter=ContainerFilter(space=selected_space))
            ]
            block_if_views_reference_containers(
                client=client,
                container_ids=container_ids,
                is_in_scope=partial(_view_is_in_space, selected_space),
            )

        if not dry_run:
            if instance_count > 0:
                project_instance_statistics = client.data_modeling.statistics.project().instances
                validate_soft_delete_headroom(
                    project_instance_statistics, instance_count, action="purging this space (including its instances)"
                )
                print_soft_delete_panel(project_instance_statistics, instance_count)
                acknowledge_soft_delete = questionary.confirm(
                    "Do you understand the soft-delete resource limit impact and wish to continue?",
                    default=False,
                ).ask()
                if not acknowledge_soft_delete:
                    return DeployResults([], "purge", dry_run=dry_run)
            self._print_panel("space", selected_space)

            confirm = confirm_by_typing_project_name(f"You are about purge the {selected_space!r} space", client)
            if not confirm:
                return DeployResults([], "purge", dry_run=dry_run)

        # ValidateAuth
        if include_space or (stats.containers + stats.views + stats.data_models) > 0:
            # We check for write even in dry-run mode. This is because dry-run is expected to fail
            # if the user cannot perform the purge.
            validator.data_model(["read", "write"], spaces={selected_space})
        if (stats.nodes + stats.edges) > 0:
            validator.instances(["read", "write"], spaces={selected_space})

        to_delete = self._create_to_delete_list_purge_space(client, delete_datapoints, delete_file_content, stats)
        if dry_run:
            results = DeployResults([], "purge", dry_run=True)
            for item in to_delete:
                results[item.display_name] = ResourceDeployResult(item.display_name, deleted=item.total)
            if include_space:
                space_loader = SpaceCRUD.create_loader(client)
                results[space_loader.display_name] = ResourceDeployResult(space_loader.display_name, deleted=1)
        else:
            results = self._delete_resources(to_delete, client, verbose, selected_space, None, log_dir)
            if include_space:
                self._delete_space(client, selected_space, results)
        print(results.counts_table(exclude_columns={"Created", "Changed", "Total"}))
        return results

    @staticmethod
    def _create_to_delete_list_purge_space(
        client: ToolkitClient, delete_datapoints: bool, delete_file_content: bool, stats: SpaceStatistics
    ) -> list[ToDelete]:
        config = client.config
        to_delete = [
            EdgeToDelete(
                EdgeCRUD.create_loader(client), stats.edges, config.create_api_url("/models/instances/delete")
            ),
            NodesToDelete(
                NodeCRUD.create_loader(client),
                stats.nodes,
                config.create_api_url(
                    "/models/instances/delete",
                ),
                delete_datapoints=delete_datapoints,
                delete_file_content=delete_file_content,
            ),
            DataModelingToDelete(
                DataModelIO.create_loader(client),
                stats.data_models,
                config.create_api_url("/models/datamodels/delete"),
            ),
            DataModelingToDelete(
                ViewIO.create_loader(client), stats.views, config.create_api_url("/models/views/delete")
            ),
            DataModelingToDelete(
                ContainerCRUD.create_loader(client),
                stats.containers,
                config.create_api_url("/models/containers/delete"),
            ),
        ]
        return to_delete

    def _delete_space(self, client: ToolkitClient, selected_space: str, results: DeployResults) -> None:
        space_loader = SpaceCRUD.create_loader(client)
        try:
            space_loader.delete([SpaceId(space=selected_space)])
            print(f"Space {selected_space} deleted")
        except ToolkitAPIError as e:
            self.warn(HighSeverityWarning(f"Failed to delete space {selected_space!r}: {e}"))
        else:
            results[space_loader.display_name] = ResourceDeployResult(space_loader.display_name, deleted=1)

    def _delete_resources(
        self,
        delete_plan: list[ToDelete],
        client: ToolkitClient,
        verbose: bool,
        space: str | None,
        data_set_external_id: str | None,
        log_dir: Path,
    ) -> DeployResults:
        results = DeployResults([], "purge", dry_run=False)
        console = Console()
        with (
            HTTPClient(client.config, max_retries=10) as delete_client,
            NDJsonWriter(
                log_dir, kind="PurgeLogs", default_filestem=self._log_filestem(), compression=Uncompressed
            ) as log_writer,
        ):
            logger = FileWithAggregationLogger(log_writer)
            for step in delete_plan:
                if step.total == 0:
                    results[step.display_name] = ResourceDeployResult(step.display_name, deleted=0)
                    continue
                logger.reset()
                executor = ProducerWorkerExecutor[Page[ResourceResponseProtocol], Page[JsonVal]](
                    download_iterable=self._iterate_batch(
                        step, space, data_set_external_id, batch_size=self.BATCH_SIZE_DM, logger=logger
                    ),
                    process=step.get_process_function(client, logger),
                    write=partial(
                        self._purge_batch,
                        delete_item=step,
                        delete_url=step.delete_url,
                        delete_client=delete_client,
                        logger=logger,
                    ),
                    max_queue_size=10,
                    total_item_count=step.total,
                    download_description=f"Downloading {step.display_name}",
                    process_description=f"Preparing {step.display_name} for deletion",
                    write_description=f"Deleting {step.display_name}",
                    console=console,
                )
                executor.run()
                item_result = logger.finalize(is_dry_run=False)

                display_item_results(item_result, title=f"{step.display_name} purge results", console=console)
                self.tracker.track(
                    DataTracking.from_item_results("PurgeResult", step.crud.display_name, item_result), client
                )
                logger.write_success()
                logger.force_write()
                # Adapt item results to ResourceDeployResult. It is another refactoring to remove the
                # ResourceDeployResult (together with deployv1)
                delete_results = DeleteResults.from_item_results(item_result)
                results[step.display_name] = ResourceDeployResult(
                    step.display_name, deleted=delete_results.deleted, unchanged=delete_results.failed
                )
                executor.raise_on_error()
        return results

    @staticmethod
    def _iterate_batch(
        step: ToDelete,
        selected_space: str | None,
        data_set_external_id: str | None,
        batch_size: int,
        logger: FileWithAggregationLogger,
    ) -> Iterable[Page[ResourceResponseProtocol]]:
        batch: list[DataItem[ResourceResponseProtocol]] = []
        for resource in step.crud.iterate(space=selected_space, data_set_external_id=data_set_external_id):
            tracking_id = step.get_tracking_id(resource)
            batch.append(DataItem(tracking_id=tracking_id, item=resource))
            if len(batch) >= batch_size:
                logger.register([item.tracking_id for item in batch])
                yield Page(worker_id="main", items=batch)
                batch = []
        if batch:
            logger.register([item.tracking_id for item in batch])
            yield Page(worker_id="main", items=batch)

    @staticmethod
    def _purge_batch(
        page: Page[JsonVal],
        delete_item: ToDelete,
        delete_url: str,
        delete_client: HTTPClient,
        logger: FileWithAggregationLogger,
    ) -> None:
        if not page.items:
            return

        responses = delete_client.request_items_retries(
            ItemsRequest(
                endpoint_url=delete_url,
                method="POST",
                items=page.items,
                extra_body_fields=delete_item.get_extra_fields(),
            )
        )
        for response in responses:
            if isinstance(response, ItemsSuccessResponse):
                pass
            elif isinstance(response, ItemsFailedRequest):
                for id in response.ids:
                    logger.log(
                        LogEntryV2(
                            id=id,
                            severity=Severity.failure,
                            label="Failed request",
                            message=response.error_message,
                        )
                    )
            elif isinstance(response, ItemsFailedResponse):
                for id in response.ids:
                    logger.log(
                        LogEntryV2(
                            id=id,
                            severity=Severity.failure,
                            label=f"Failed response {response.status_code}",
                            message=response.error_message,
                        )
                    )

    def dataset(
        self,
        client: ToolkitClient,
        selected_data_set_external_id: str,
        log_dir: Path,
        archive_dataset: bool = False,
        include_data: bool = True,
        include_configurations: bool = False,
        asset_recursive: bool = False,
        dry_run: bool = False,
        auto_yes: bool = False,
        verbose: bool = False,
    ) -> DeployResults:
        """Purge a dataset and all its content

        Args:
            client: The ToolkitClient to use
            selected_data_set_external_id:  The external ID of the dataset to purge
            archive_dataset:  Whether to archive the dataset itself after the purge
            include_data: Whether to include data (assets, events, time series, files, sequences, 3D models, relationships, labels) in the purge
            include_configurations: Whether to include configurations (workflows, transformations, extraction pipelines) in the purge
            asset_recursive: Whether to recursively delete assets.
            dry_run: Whether to perform a dry run
            auto_yes:  Whether to automatically confirm the purge
            verbose:  Whether to print verbose output

        Returns:
            DeployResults: The results of the purge operation

        """
        # Warning Messages
        if not dry_run:
            self._print_panel("dataSet", selected_data_set_external_id)
        if not dry_run and not auto_yes:
            confirm = self._confirm_purge(
                f"You are about t purge the {selected_data_set_external_id!r} dataSet", client
            )
            if not confirm:
                return DeployResults([], "purge", dry_run=dry_run)

        # Validate Auth
        validator = ValidateAccess(client, "purge")
        data_set_id = client.lookup.data_sets.id(selected_data_set_external_id)
        if data_set_id is None:
            raise ToolkitMissingResourceError(f"DataSet {selected_data_set_external_id!r} does not exist")
        action = cast(Sequence[Literal["read", "write"]], ["read"] if dry_run else ["read", "write"])
        if include_data:
            # Check asset, events, time series, files, and sequences access, relationships, labels, 3D access.
            validator.dataset_data(action, dataset_ids={data_set_id})
        if include_configurations:
            # Check workflow, transformations, extraction pipeline access
            validator.dataset_configurations(action, dataset_ids={data_set_id})

        to_delete: list[ToDelete] = self._create_to_delete_list_purge_dataset(
            client,
            include_data,
            include_configurations,
            selected_data_set_external_id,
            asset_recursive,
        )
        if dry_run:
            results = DeployResults([], "purge", dry_run=True)
            for item in to_delete:
                results[item.display_name] = ResourceDeployResult(item.display_name, deleted=item.total)
        else:
            results = self._delete_resources(to_delete, client, verbose, None, selected_data_set_external_id, log_dir)
        print(results.counts_table(exclude_columns={"Created", "Changed", "Total"}))
        if archive_dataset and not dry_run:
            self._archive_dataset(client, selected_data_set_external_id)
        return results

    @staticmethod
    def _archive_dataset(client: ToolkitClient, data_set: str) -> None:
        archived = (
            DataSetUpdate(external_id=data_set)
            .external_id.set(str(uuid.uuid4()))
            .metadata.add({"archived": "true"})
            .write_protected.set(True)
        )
        client.data_sets.update(archived)
        print(f"DataSet {data_set} archived")

    @staticmethod
    def _create_to_delete_list_purge_dataset(
        client: ToolkitClient,
        include_data: bool,
        include_configurations: bool,
        data_set_external_id: str,
        asset_recursive: bool,
    ) -> list[ToDelete]:
        config = client.config
        to_delete: list[ToDelete] = []

        if include_data:
            three_d_crud = ThreeDModelCRUD.create_loader(client)
            to_delete.extend(
                [
                    ExternalIdToDelete(
                        RelationshipIO.create_loader(client),
                        RelationshipAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/relationships/delete"),
                    ),
                    IdResourceToDelete(
                        EventIO.create_loader(client),
                        EventAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/events/delete"),
                    ),
                    IdResourceToDelete(
                        FileMetadataCRUD.create_loader(client),
                        FileAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/files/delete"),
                    ),
                    IdResourceToDelete(
                        TimeSeriesCRUD.create_loader(client),
                        TimeSeriesAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/timeseries/delete"),
                    ),
                    IdResourceToDelete(
                        SequenceIO.create_loader(client),
                        SequenceAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/sequences/delete"),
                    ),
                    IdResourceToDelete(
                        three_d_crud,
                        sum(1 for _ in three_d_crud.iterate(data_set_external_id=data_set_external_id)),
                        config.create_api_url("/3d/models/delete"),
                    ),
                    AssetToDelete(
                        AssetIO.create_loader(client),
                        AssetAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/assets/delete"),
                        recursive=asset_recursive,
                    ),
                    ExternalIdToDelete(
                        LabelIO.create_loader(client),
                        LabelCountAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/labels/delete"),
                    ),
                ]
            )
        if include_configurations:
            transformation_crud = TransformationIO.create_loader(client)
            workflow_crud = WorkflowIO.create_loader(client)
            extraction_pipeline_crud = ExtractionPipelineIO.create_loader(client)

            to_delete.extend(
                [
                    IdResourceToDelete(
                        transformation_crud,
                        sum(1 for _ in transformation_crud.iterate(data_set_external_id=data_set_external_id)),
                        config.create_api_url("/transformations/delete"),
                    ),
                    ExternalIdToDelete(
                        workflow_crud,
                        sum(1 for _ in workflow_crud.iterate(data_set_external_id=data_set_external_id)),
                        config.create_api_url("/workflows/delete"),
                    ),
                    IdResourceToDelete(
                        extraction_pipeline_crud,
                        sum(1 for _ in extraction_pipeline_crud.iterate(data_set_external_id=data_set_external_id)),
                        config.create_api_url("/extpipes/delete"),
                    ),
                ]
            )
        return to_delete

    @staticmethod
    def _print_panel(resource_type: str, resource: str) -> None:
        print(
            Panel(
                f"[red]WARNING:[/red] This operation [bold]cannot be undone[/bold]! "
                f"Resources in {resource!r} are permanently deleted",
                style="bold",
                title=f"Purge {resource_type}",
                title_align="left",
                border_style="red",
                expand=False,
            )
        )

    def instances(
        self,
        client: ToolkitClient,
        selector: InstanceSelector,
        log_dir: Path,
        dry_run: bool = False,
        unlink: bool = True,
        verbose: bool = False,
    ) -> DeleteResults:
        """Purge instances"""
        io = InstanceIO(client)
        console = client.console
        validator = ValidateAccess(client, default_operation="purge")
        self.validate_instance_access(validator, selector.get_instance_spaces())
        if unlink:
            self.validate_timeseries_access(validator)
            self.validate_file_access(validator)

        total = io.count(selector)
        if total is None or total == 0:
            print("No instances found.")
            return DeleteResults()
        if not dry_run:
            project_instance_statistics = client.data_modeling.statistics.project().instances
            validate_soft_delete_headroom(project_instance_statistics, total, action="purging the selected instances")
            print_soft_delete_panel(project_instance_statistics, total)
            acknowledge_soft_delete = questionary.confirm(
                "Do you understand the soft-delete resource limit impact and wish to continue?",
                default=False,
            ).ask()
            if not acknowledge_soft_delete:
                return DeleteResults()
            self._print_panel("instances", str(selector))

            confirm_purge = confirm_by_typing_project_name(
                f"You are about to purge all {total:,} instances in {selector!s}", client
            )
            if not confirm_purge:
                return DeleteResults()

        with (
            HTTPClient(config=client.config) as delete_client,
            NDJsonWriter(
                log_dir, kind="PurgeLogs", default_filestem=self._log_filestem(), compression=Uncompressed
            ) as log_writer,
        ):
            logger = FileWithAggregationLogger(log_writer)
            io.logger = logger

            process: Callable[[Page[InstanceDefinitionId]], Page[InstanceDefinitionId]] = self._no_op
            if unlink:
                process = partial(self._unlink_prepare, client=client, dry_run=dry_run, logger=logger)

            process_str = "Would be unlinking" if dry_run else "Unlinking"
            write_str = "Would be deleting" if dry_run else "Deleting"

            executor = ProducerWorkerExecutor[Page[InstanceDefinitionId], Page[InstanceDefinitionId]](
                download_iterable=io.download_ids(selector),
                process=process,
                write=partial(self._delete_instance_ids, dry_run=dry_run, delete_client=delete_client, logger=logger),
                total_item_count=total,
                max_queue_size=10,
                download_description=f"Retrieving instances from {selector!s}",
                process_description=f"{process_str} instances from files/timeseries" if unlink else "",
                write_description=f"{write_str} instances",
                console=console,
            )
            executor.run()
            items_results = logger.finalize(is_dry_run=dry_run)
            display_item_results(items_results, title=f"Finished {selector.display_name}", console=console)
            self.tracker.track(DataTracking.from_item_results("PurgeResult", io.KIND, items_results), client)
            logger.write_success()
            logger.force_write()
            executor.raise_on_error()

        return DeleteResults.from_item_results(items_results)

    def _log_filestem(self) -> str:
        log_filestem = f"purge_{date.today().strftime('%Y%m%d')}"
        return log_filestem

    def validate_instance_access(self, validator: ValidateAccess, instance_spaces: list[str] | None) -> None:
        space_ids = validator.instances(
            ["read", "write"], spaces=set(instance_spaces) if instance_spaces else None, operation="purge"
        )
        if space_ids is None:
            # Full access
            return
        self.warn(
            LimitedAccessWarning(
                f"You can only purge instances in the following instance spaces: {humanize_collection(space_ids)}."
            )
        )
        return

    def validate_model_access(self, validator: ValidateAccess, view: list[str] | None) -> None:
        space = view[0] if isinstance(view, list) and view and isinstance(view[0], str) else None
        if space_ids := validator.data_model(["read"], spaces={space} if space else None):
            self.warn(
                LimitedAccessWarning(
                    f"You can only select views in the {len(space_ids)} spaces you have access to: {humanize_collection(space_ids)}."
                )
            )

    def validate_timeseries_access(self, validator: ValidateAccess) -> None:
        try:
            ids_by_scope = validator.timeseries(["read", "write"], operation="unlink")
        except AuthorizationError as e:
            self.warn(HighSeverityWarning(f"You cannot unlink time series. You need read and write access: {e!s}"))
            return
        if ids_by_scope is None:
            return
        scope_str = humanize_collection(
            [f"{scope_name} ({humanize_collection(ids)})" for scope_name, ids in ids_by_scope.items()],
            bind_word="and",
        )
        self.warn(LimitedAccessWarning(f"You can only unlink time series in the following scopes: {scope_str}."))

    def validate_file_access(self, validator: ValidateAccess) -> None:
        try:
            ids_by_scope = validator.files(["read", "write"], operation="unlink")
        except AuthorizationError as e:
            self.warn(HighSeverityWarning(f"You cannot unlink files. You need read and write access: {e!s}"))
            return
        if ids_by_scope is None:
            return
        scope_str = humanize_collection(
            [f"{scope_name} ({humanize_collection(ids)})" for scope_name, ids in ids_by_scope.items()],
            bind_word="and",
        )
        self.warn(LimitedAccessWarning(f"You can only unlink files in the following scopes: {scope_str}."))

    @staticmethod
    def _no_op(instance_ids: Page[InstanceDefinitionId]) -> Page[InstanceDefinitionId]:
        return instance_ids

    def _unlink_prepare(
        self,
        instance_ids: Page[InstanceDefinitionId],
        client: ToolkitClient,
        dry_run: bool,
        logger: FileWithAggregationLogger,
    ) -> Page[InstanceDefinitionId]:
        self._unlink_timeseries(instance_ids, client, dry_run, logger)
        self._unlink_files(instance_ids, client, dry_run, logger)
        return instance_ids

    @staticmethod
    def _delete_instance_ids(
        page: Page[InstanceDefinitionId],
        dry_run: bool,
        delete_client: HTTPClient,
        logger: FileWithAggregationLogger,
    ) -> None:
        if dry_run:
            logger.apply_to_all_unprocessed("Ready to delete", Severity.info)
            return

        responses = delete_client.request_items_retries(
            ItemsRequest(
                endpoint_url=delete_client.config.create_api_url("/models/instances/delete"),
                method="POST",
                items=page.items,
            )
        )
        for response in responses:
            if isinstance(response, ItemsFailedRequest):
                for id in response.ids:
                    logger.log(
                        LogEntryV2(
                            id=id, severity=Severity.failure, label="Failed request", message=response.error_message
                        )
                    )
            elif isinstance(response, ItemsFailedResponse):
                for id in response.ids:
                    logger.log(
                        LogEntryV2(
                            id=id,
                            severity=Severity.failure,
                            label=f"Failed response {response.status_code}",
                            message=response.error_message,
                        )
                    )

    @staticmethod
    def _unlink_timeseries(
        page: Page[InstanceDefinitionId],
        client: ToolkitClient,
        dry_run: bool,
        logger: FileWithAggregationLogger,
    ) -> None:
        node_items = {InstanceId(instance_id=item.item): item for item in page.items if isinstance(item.item, NodeId)}
        if node_items:
            timeseries = client.tool.timeseries.retrieve(list(node_items.keys()), ignore_unknown_ids=True)
            migrated_timeseries_ids = {
                InternalId(id=ts.id): InstanceId(instance_id=ts.instance_id)
                for ts in timeseries
                if ts.instance_id and ts.pending_instance_id
            }
            if not dry_run and timeseries:
                client.tool.timeseries.unlink_instance_ids(list(migrated_timeseries_ids.keys()))
                for internal_id, value in migrated_timeseries_ids.items():
                    data_item = node_items[value]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Unlinked timeseries",
                            severity=Severity.info,
                            message=f"Unlinked TimeSeries with internal ID {internal_id!s} from Node {data_item.item!r}",
                        )
                    )
            elif migrated_timeseries_ids:
                for internal_id, value in migrated_timeseries_ids.items():
                    data_item = node_items[value]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Ready to unlink timeseries",
                            severity=Severity.info,
                            message=f"Ready to unlink TimeSeries with internal ID {internal_id!s} from Node {data_item.item!r}",
                        )
                    )
        return None

    @staticmethod
    def _unlink_files(
        page: Page[InstanceDefinitionId],
        client: ToolkitClient,
        dry_run: bool,
        logger: FileWithAggregationLogger,
    ) -> None:
        node_items = {InstanceId(instance_id=item.item): item for item in page.items if isinstance(item.item, NodeId)}
        if node_items:
            files = client.tool.filemetadata.retrieve(list(node_items.keys()), ignore_unknown_ids=True)
            migrated_file_ids = {
                InternalId(id=file.id): InstanceId(instance_id=file.instance_id)
                for file in files
                if file.instance_id and file.pending_instance_id
            }
            if not dry_run and files:
                client.tool.filemetadata.unlink_instance_ids(list(migrated_file_ids.keys()))
                for internal_id, value in migrated_file_ids.items():
                    data_item = node_items[value]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Unlinked file",
                            severity=Severity.info,
                            message=f"Unlinked File with internal ID {internal_id!s} from Node {data_item.item!r}",
                        )
                    )
            elif migrated_file_ids:
                for internal_id, value in migrated_file_ids.items():
                    data_item = node_items[value]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Ready to unlink file",
                            severity=Severity.info,
                            message=f"Ready to unlink File with internal ID {internal_id!s} from Node {data_item.item!r}",
                        )
                    )
        return None
