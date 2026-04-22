import re
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any, Literal, cast

import questionary
from cognite.client.data_classes import DataSetUpdate
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics, SpaceStatistics
from cognite.client.exceptions import CogniteAPIError
from pydantic import JsonValue
from rich import print
from rich.console import Console
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestItem
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsFailedResponse,
    ItemsRequest,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import (
    InstanceDefinitionId,
    InstanceId,
    InternalId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId, NodeResponse, SpaceId
from cognite_toolkit._cdf_tk.constants import DMS_SOFT_DELETED_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.data_classes import DeployResults, ResourceDeployResult
from cognite_toolkit._cdf_tk.dataio import InstanceIO
from cognite_toolkit._cdf_tk.dataio._base import DataItem
from cognite_toolkit._cdf_tk.dataio.logger import (
    FileWithAggregationLogger,
    LogEntryV2,
    Severity,
    display_item_results,
)
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceSelector
from cognite_toolkit._cdf_tk.exceptions import (
    AuthorizationError,
    ToolkitMissingResourceError,
    ToolkitValueError,
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


def validate_soft_delete_purge_headroom(
    instance_statistics: InstanceStatistics,
    instances_to_soft_delete: int,
    *,
    action: str,
) -> None:
    """Abort if the purge would exhaust the soft-delete resource limit."""
    if instances_to_soft_delete <= 0:
        return
    used = instance_statistics.soft_deleted_instances
    limit = instance_statistics.soft_deleted_instances_limit
    margin = DMS_SOFT_DELETED_INSTANCE_LIMIT_MARGIN
    projected = used + instances_to_soft_delete
    headroom_after = limit - projected
    if headroom_after < margin:
        headroom_clause = (
            f"leaving only {headroom_after:,} instances of headroom, which is less than the required margin of {margin:,}."
            if headroom_after >= 0
            else f"exceeding the limit by {-headroom_after:,} instances."
        )
        raise ToolkitValueError(
            f"Cannot proceed with {action}, not enough soft-deleted instance capacity available. "
            f"Currently {used:,} of {limit:,} instances are soft-deleted. Performing this operation would add up to "
            f"{instances_to_soft_delete:,} more (projected total: {projected:,}), {headroom_clause} "
            f"Reduce what you purge, or wait for soft-deleted data to expire before retrying "
            f"(see: https://docs.cognite.com/cdf/dm/dm_concepts/dm_ingestion#soft-deletion for details)."
        )


@dataclass
class DeleteResults:
    deleted: int = 0
    failed: int = 0


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
        self,
        client: ToolkitClient,
        console: Console,
        verbose: bool,
        process_results: ResourceDeployResult,
        logger: FileWithAggregationLogger,
    ) -> Callable[[list[DataItem[ResourceResponseProtocol]]], list[DataItem[dict[str, Any]]]]:
        raise NotImplementedError()

    def get_extra_fields(self) -> dict[str, JsonVal]:
        return {}


@dataclass
class DataModelingToDelete(ToDelete):
    def get_process_function(
        self,
        client: ToolkitClient,
        console: Console,
        verbose: bool,
        process_results: ResourceDeployResult,
        logger: FileWithAggregationLogger,
    ) -> Callable[[list[DataItem[ResourceResponseProtocol]]], list[DataItem[dict[str, Any]]]]:
        def as_id(chunk: list[DataItem[ResourceResponseProtocol]]) -> list[DataItem[dict[str, Any]]]:
            # We know that all data modeling resources implement as_id
            return [
                DataItem(tracking_id=item.tracking_id, item=item.item.as_id().dump())  # type: ignore[attr-defined]
                for item in chunk
            ]

        return as_id


@dataclass
class EdgeToDelete(ToDelete):
    def get_process_function(
        self,
        client: ToolkitClient,
        console: Console,
        verbose: bool,
        process_results: ResourceDeployResult,
        logger: FileWithAggregationLogger,
    ) -> Callable[[list[DataItem[ResourceResponseProtocol]]], list[DataItem[dict[str, Any]]]]:
        def as_id(chunk: list[DataItem[ResourceResponseProtocol]]) -> list[DataItem[dict[str, Any]]]:
            return [
                DataItem(
                    tracking_id=item.tracking_id,
                    item={"space": item.item.space, "externalId": item.item.external_id, "instanceType": "edge"},  # type: ignore[attr-defined]
                )
                for item in chunk
            ]

        return as_id


@dataclass
class NodesToDelete(ToDelete):
    delete_datapoints: bool
    delete_file_content: bool

    def get_process_function(
        self,
        client: ToolkitClient,
        console: Console,
        verbose: bool,
        process_results: ResourceDeployResult,
        logger: FileWithAggregationLogger,
    ) -> Callable[[list[DataItem[ResourceResponseProtocol]]], list[DataItem[dict[str, Any]]]]:
        def check_for_data(chunk: list[DataItem[ResourceResponseProtocol]]) -> list[DataItem[dict[str, Any]]]:
            # Build mapping from NodeId to DataItem for tracking
            nodes_by_id: dict[NodeId, DataItem[ResourceResponseProtocol]] = {}
            instance_ids: list[InstanceId] = []
            for item in chunk:
                node = cast(NodeResponse, item.item)
                node_id = node.as_id()
                nodes_by_id[node_id] = item
                instance_ids.append(InstanceId(instance_id=node_id))

            found_ids: set[NodeId] = set()
            if not self.delete_datapoints:
                timeseries = client.tool.timeseries.retrieve(instance_ids, ignore_unknown_ids=True)
                found_ids |= {ts.instance_id for ts in timeseries if ts.instance_id is not None}
            if not self.delete_file_content:
                files = client.tool.filemetadata.retrieve(instance_ids, ignore_unknown_ids=True)
                found_ids |= {f.instance_id for f in files if f.instance_id is not None}

            if found_ids and verbose:
                console.print(f"Skipping {len(found_ids)} nodes as they have datapoints or file content")

            # Log skipped items
            for node_id in found_ids:
                if node_id in nodes_by_id:
                    data_item = nodes_by_id[node_id]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Skipped - has linked data",
                            severity=Severity.skipped,
                            message="Node has datapoints or file content and was not deleted",
                        )
                    )

            process_results.unchanged += len(found_ids)
            result: list[DataItem[dict[str, Any]]] = []
            for node_id, data_item in nodes_by_id.items():
                if node_id not in found_ids:
                    dumped = node_id.dump(include_instance_type=True)
                    result.append(DataItem(tracking_id=data_item.tracking_id, item=dumped))
            return result

        return check_for_data


@dataclass
class IdResourceToDelete(ToDelete):
    def get_process_function(
        self,
        client: ToolkitClient,
        console: Console,
        verbose: bool,
        process_results: ResourceDeployResult,
        logger: FileWithAggregationLogger,
    ) -> Callable[[list[DataItem[ResourceResponseProtocol]]], list[DataItem[dict[str, Any]]]]:
        def as_id(chunk: list[DataItem[ResourceResponseProtocol]]) -> list[DataItem[dict[str, Any]]]:
            # We know that all id resources have an id attribute
            return [
                DataItem(tracking_id=item.tracking_id, item={"id": item.item.id})  # type: ignore[attr-defined]
                for item in chunk
            ]

        return as_id


@dataclass
class ExternalIdToDelete(ToDelete):
    def get_process_function(
        self,
        client: ToolkitClient,
        console: Console,
        verbose: bool,
        process_results: ResourceDeployResult,
        logger: FileWithAggregationLogger,
    ) -> Callable[[list[DataItem[ResourceResponseProtocol]]], list[DataItem[dict[str, Any]]]]:
        def as_external_id(chunk: list[DataItem[ResourceResponseProtocol]]) -> list[DataItem[dict[str, Any]]]:
            # We know that all external id resources have an external_id attribute
            return [
                DataItem(tracking_id=item.tracking_id, item={"externalId": item.item.external_id})  # type: ignore[attr-defined]
                for item in chunk
            ]

        return as_external_id


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

        if not dry_run:
            if instance_count > 0:
                project_instance_statistics = client.data_modeling.statistics.project().instances
                validate_soft_delete_purge_headroom(
                    project_instance_statistics, instance_count, action="purging this space (including its instances)"
                )
                self._print_instance_purge_soft_delete_panel(project_instance_statistics, instance_count)
                acknowledge_soft_delete = questionary.confirm(
                    "Do you understand the soft-delete resource limit impact and wish to continue?",
                    default=False,
                ).ask()
                if not acknowledge_soft_delete:
                    return DeployResults([], "purge", dry_run=dry_run)
            self._print_panel("space", selected_space)

            confirm = self._confirm_purge(f"You are about purge the {selected_space!r} space", client)
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
            log_dir = Path.cwd()
            log_filestem = self._create_purge_logfile_stem(log_dir)
            console = client.console

            with (
                NDJsonWriter(
                    log_dir, kind="PurgeIssues", default_filestem=log_filestem, compression=Uncompressed
                ) as log_file,
                FileWithAggregationLogger(log_file) as logger,
            ):
                results = self._delete_resources(
                    to_delete, client, verbose, selected_space, None, dry_run=False, logger=logger, console=console
                )
                if include_space:
                    self._delete_space(client, selected_space, results, logger)
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

    def _delete_space(
        self, client: ToolkitClient, selected_space: str, results: DeployResults, logger: FileWithAggregationLogger
    ) -> None:
        space_loader = SpaceCRUD.create_loader(client)
        logger.register([selected_space])
        try:
            space_loader.delete([SpaceId(space=selected_space)])
            print(f"Space {selected_space} deleted")
        except CogniteAPIError as e:
            self.warn(HighSeverityWarning(f"Failed to delete space {selected_space!r}: {e}"))
            logger.log(
                LogEntryV2(
                    id=selected_space,
                    label="Delete space failed",
                    severity=Severity.failure,
                    message=str(e),
                )
            )
        else:
            results[space_loader.display_name] = ResourceDeployResult(space_loader.display_name, deleted=1)

    def _delete_resources(
        self,
        to_delete: list[ToDelete],
        client: ToolkitClient,
        verbose: bool,
        space: str | None,
        data_set_external_id: str | None,
        dry_run: bool,
        logger: FileWithAggregationLogger,
        console: Console,
    ) -> DeployResults:
        results = DeployResults([], "purge", dry_run=dry_run)
        with HTTPClient(client.config, max_retries=10) as delete_client:
            for item in to_delete:
                if item.total == 0:
                    results[item.display_name] = ResourceDeployResult(item.display_name, deleted=0)
                    continue

                logger.reset()
                # Two results objects since they are updated concurrently
                process_results = ResourceDeployResult(item.display_name)
                write_results = ResourceDeployResult(item.display_name)

                def download_with_tracking() -> Iterable[list[DataItem[ResourceResponseProtocol]]]:
                    for batch in self._iterate_batch(
                        item.crud, space, data_set_external_id, batch_size=self.BATCH_SIZE_DM
                    ):
                        items = [
                            DataItem(
                                tracking_id=self._get_resource_tracking_id(item.crud, resource),
                                item=resource,
                            )
                            for resource in batch
                        ]
                        logger.register([data_item.tracking_id for data_item in items])
                        yield items

                executor = ProducerWorkerExecutor[
                    list[DataItem[ResourceResponseProtocol]], list[DataItem[dict[str, Any]]]
                ](
                    download_iterable=download_with_tracking(),
                    process=item.get_process_function(client, console, verbose, process_results, logger),
                    write=self._purge_batch_tracked(
                        item, item.delete_url, delete_client, write_results, dry_run, logger
                    ),
                    max_queue_size=10,
                    total_item_count=item.total,
                    download_description=f"Downloading {item.display_name}",
                    process_description=f"Preparing {item.display_name} for deletion",
                    write_description=f"Deleting {item.display_name}",
                    console=console,
                )
                executor.run()
                write_results += process_results
                results[item.display_name] = write_results

                # Finalize and display results for this resource type
                items_results = logger.finalize(dry_run)
                display_item_results(items_results, title=f"Purge {item.display_name}", console=console)

                if executor.error_occurred:
                    if verbose and executor.error_traceback:
                        executor.print_traceback()
                    self.warn(
                        HighSeverityWarning(f"Failed to delete all {item.display_name}. {executor.error_message}")
                    )
        return results

    @staticmethod
    def _get_resource_tracking_id(crud: ResourceIO, resource: ResourceResponseProtocol) -> str:
        """Get a tracking ID for a resource based on its type."""
        # Try external_id first, then id, then fall back to string representation
        if hasattr(resource, "external_id") and resource.external_id is not None:
            return str(resource.external_id)
        if hasattr(resource, "space") and hasattr(resource, "external_id"):
            return f"{resource.space}:{resource.external_id}"
        if hasattr(resource, "id") and resource.id is not None:
            return str(resource.id)
        return str(resource)

    @staticmethod
    def _iterate_batch(
        crud: ResourceIO, selected_space: str | None, data_set_external_id: str | None, batch_size: int
    ) -> Iterable[list[ResourceResponseProtocol]]:
        batch: list[ResourceResponseProtocol] = []
        for resource in crud.iterate(space=selected_space, data_set_external_id=data_set_external_id):
            batch.append(resource)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    @staticmethod
    def _purge_batch(
        delete_item: ToDelete, delete_url: str, delete_client: HTTPClient, result: ResourceDeployResult
    ) -> Callable[[list[JsonVal]], None]:
        crud = delete_item.crud

        def as_id(item: JsonVal) -> str:
            try:
                return str(crud.get_id(item))
            except KeyError:
                # Fallback to internal ID
                return str(crud.get_internal_id(item))

        def process(items: list[JsonVal]) -> None:
            responses = delete_client.request_items_retries(
                ItemsRequest(
                    endpoint_url=delete_url,
                    method="POST",
                    items=[DeleteItem(item=item, as_id_fun=as_id) for item in items],
                    extra_body_fields=delete_item.get_extra_fields(),
                )
            )
            for response in responses:
                if isinstance(response, ItemsSuccessResponse):
                    result.deleted += len(response.ids)
                else:
                    result.unchanged += len(response.ids)

        return process

    @staticmethod
    def _purge_batch_tracked(
        delete_item: ToDelete,
        delete_url: str,
        delete_client: HTTPClient,
        result: ResourceDeployResult,
        dry_run: bool,
        logger: FileWithAggregationLogger,
    ) -> Callable[[list[DataItem[dict[str, Any]]]], None]:
        """Purge a batch of items while tracking success/failure with the logger."""
        crud = delete_item.crud

        def as_id(item: JsonVal) -> str:
            try:
                return str(crud.get_id(item))
            except KeyError:
                # Fallback to internal ID
                return str(crud.get_internal_id(item))

        def process(items: list[DataItem[dict[str, Any]]]) -> None:
            if dry_run:
                result.deleted += len(items)
                return

            # Build tracking ID mapping
            tracking_by_id: dict[str, str] = {}
            delete_items: list[DeleteItem] = []
            for data_item in items:
                item_id = as_id(data_item.item)
                tracking_by_id[item_id] = data_item.tracking_id
                delete_items.append(DeleteItem(item=data_item.item, as_id_fun=as_id))

            responses = delete_client.request_items_retries(
                ItemsRequest(
                    endpoint_url=delete_url,
                    method="POST",
                    items=delete_items,
                    extra_body_fields=delete_item.get_extra_fields(),
                )
            )
            for response in responses:
                if isinstance(response, ItemsSuccessResponse):
                    result.deleted += len(response.ids)
                    # Success items don't need logging - they remain without log entries
                elif isinstance(response, ItemsFailedResponse):
                    result.unchanged += len(response.ids)
                    # Log failures
                    for failed_id in response.ids:
                        tracking_id = tracking_by_id.get(failed_id, failed_id)
                        logger.log(
                            LogEntryV2(
                                id=tracking_id,
                                label="Delete failed",
                                severity=Severity.failure,
                                message=response.error_message,
                            )
                        )
                else:
                    result.unchanged += len(response.ids)
                    for failed_id in response.ids:
                        tracking_id = tracking_by_id.get(failed_id, failed_id)
                        logger.log(
                            LogEntryV2(
                                id=tracking_id,
                                label="Delete failed",
                                severity=Severity.failure,
                                message="Unknown error during deletion",
                            )
                        )

        return process

    def dataset(
        self,
        client: ToolkitClient,
        selected_data_set_external_id: str,
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
            log_dir = Path.cwd()
            log_filestem = self._create_purge_logfile_stem(log_dir)
            console = client.console

            with (
                NDJsonWriter(
                    log_dir, kind="PurgeIssues", default_filestem=log_filestem, compression=Uncompressed
                ) as log_file,
                FileWithAggregationLogger(log_file) as logger,
            ):
                results = self._delete_resources(
                    to_delete,
                    client,
                    verbose,
                    None,
                    selected_data_set_external_id,
                    dry_run=False,
                    logger=logger,
                    console=console,
                )
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

    @staticmethod
    def _print_instance_purge_soft_delete_panel(
        instance_statistics: InstanceStatistics,
        instances_to_delete: int,
    ) -> None:
        """Step 1 panel: soft-delete resource limit impact and related notices."""
        used = max(0, instance_statistics.soft_deleted_instances)
        limit = instance_statistics.soft_deleted_instances_limit
        projected = used + instances_to_delete
        remaining_after = max(0, limit - projected)
        bar_width = 44

        bar = "".join(
            "[yellow]█[/yellow]"
            if (i + 0.5) / bar_width * limit < used
            else "[bright_magenta]█[/bright_magenta]"
            if (i + 0.5) / bar_width * limit < min(projected, limit)
            else "[dim]░[/dim]"
            for i in range(bar_width)
        )
        resource_usage_bar = (
            "[yellow]█[/yellow] [dim]already soft-deleted   [/dim]"
            "[bright_magenta]█[/bright_magenta] [dim]this purge   [/dim]"
            "[dim]░ remaining[/dim]\n\n"
            f"{bar}\n"
            f"[dim]Limit [/dim][bold]{limit:,}[/bold][dim]  ·  [/dim]"
            f"[yellow]{used:,}[/yellow][dim] + [/dim][bright_magenta]{instances_to_delete:,}[/bright_magenta]"
            f"[dim] → [/dim][bold]{projected:,}[/bold][dim] total soft-deleted (est.)  ·  [/dim]"
            f"[green]{remaining_after:,}[/green][dim] remaining[/dim]"
        )

        print(
            Panel(
                "By continuing this operation you will be deleting instances, which consumes your CDF project-wide "
                "[bold]soft-delete resource limit[/bold] for instances. If that resource limit is exhausted, you will "
                "not be able to delete any more instances until the soft-deleted data expires and is hard-deleted per "
                "the retention policy, which can take multiple days (see "
                "https://docs.cognite.com/cdf/dm/dm_concepts/dm_ingestion#soft-deletion for details).\n\n"
                f"[bold]This purge targets up to {instances_to_delete:,} instance(s).[/bold] Each deleted instance "
                f"counts toward the total soft-delete limit below.\n\n{resource_usage_bar}\n\n"
                "[bold]NOTE:[/bold] Please be aware, if you intended to delete containers or views, this does not "
                "require deleting instances. You can delete or change schema resources (containers, views, data models) "
                "without purging the instance data first. Only run an instance purge when you intend to remove specific "
                "data which was either ingested by error or is no longer needed or valid.",
                title="Purging instances: Please acknowledge the following",
                title_align="left",
                border_style="yellow",
                expand=False,
            )
        )

    def instances(
        self,
        client: ToolkitClient,
        selector: InstanceSelector,
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
            validate_soft_delete_purge_headroom(
                project_instance_statistics, total, action="purging the selected instances"
            )
            self._print_instance_purge_soft_delete_panel(project_instance_statistics, total)
            acknowledge_soft_delete = questionary.confirm(
                "Do you understand the soft-delete resource limit impact and wish to continue?",
                default=False,
            ).ask()
            if not acknowledge_soft_delete:
                return DeleteResults()
            self._print_panel("instances", str(selector))

            confirm_purge = self._confirm_purge(
                f"You are about to purge all {total:,} instances in {selector!s}", client
            )
            if not confirm_purge:
                return DeleteResults()

        log_dir = Path.cwd()
        log_filestem = self._create_purge_logfile_stem(log_dir)
        results = DeleteResults()

        with (
            NDJsonWriter(
                log_dir, kind="PurgeIssues", default_filestem=log_filestem, compression=Uncompressed
            ) as log_file,
            FileWithAggregationLogger(log_file) as logger,
            HTTPClient(config=client.config) as delete_client,
        ):
            process_str = "Would be unlinking" if dry_run else "Unlinking"
            write_str = "Would be deleting" if dry_run else "Deleting"

            def download_with_tracking() -> Iterable[list[DataItem[InstanceDefinitionId]]]:
                for batch in io.download_ids(selector):
                    items = [
                        DataItem(
                            tracking_id=f"{instance_id.space}:{instance_id.external_id}",
                            item=instance_id,
                        )
                        for instance_id in batch
                    ]
                    logger.register([item.tracking_id for item in items])
                    yield items

            process: Callable[[list[DataItem[InstanceDefinitionId]]], list[DataItem[dict[str, Any]]]]
            if unlink:
                process = partial(
                    self._unlink_prepare_tracked,
                    client=client,
                    dry_run=dry_run,
                    console=console,
                    verbose=verbose,
                    logger=logger,
                )
            else:
                process = self._prepare_tracked

            executor = ProducerWorkerExecutor[list[DataItem[InstanceDefinitionId]], list[DataItem[dict[str, Any]]]](
                download_iterable=download_with_tracking(),
                process=process,
                write=partial(
                    self._delete_instance_ids_tracked,
                    dry_run=dry_run,
                    delete_client=delete_client,
                    results=results,
                    logger=logger,
                ),
                total_item_count=total,
                max_queue_size=10,
                download_description=f"Retrieving instances from {selector!s}",
                process_description=f"{process_str} instances from files/timeseries" if unlink else "",
                write_description=f"{write_str} instances",
                console=console,
            )

            executor.run()
            items_results = logger.finalize(dry_run)
            display_item_results(items_results, title=f"Purge {selector!s}", console=console)
            executor.raise_on_error()

        prefix = "Would have purged" if dry_run else "Purged"
        if results.failed == 0:
            console.print(f"{prefix} {results.deleted:,} instances in {selector!s}")
        else:
            console.print(
                f"{prefix} {results.deleted:,} instances in {selector!s}, but failed to purge {results.failed:,} instances"
            )
        return results

    @staticmethod
    def _create_purge_logfile_stem(log_dir: Path) -> str:
        """Create a filestem for the purge log file that does not conflict with existing files in the directory."""
        base_logstem = "purge-"
        existing_files = list(log_dir.glob(f"{base_logstem}*"))
        if not existing_files:
            return base_logstem

        run_pattern = re.compile(re.escape(base_logstem) + r"run(\d+)-")
        max_run = 0
        for f in existing_files:
            match = run_pattern.match(f.name)
            if match:
                max_run = max(max_run, int(match.group(1)))

        next_run = max(2, max_run + 1)
        return f"{base_logstem}run{next_run}-"

    def _confirm_purge(self, message: str, client: ToolkitClient) -> bool:
        client_project = client.config.project
        client.console.print(f"{message} in the CDF project [bold]{client_project!r}[/bold]")
        typed_project = questionary.text("To confirm, please type the name of the CDF project: ").unsafe_ask()
        if typed_project != client_project:
            client.console.print(
                f"The CDF project you typed does not match your credentials {typed_project!r}≠{client_project!r}. Exiting..."
            )
            return False

        return True

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
    def _prepare(instance_ids: Sequence[InstanceDefinitionId]) -> list[dict[str, JsonVal]]:
        output: list[dict[str, JsonVal]] = []
        for instance_id in instance_ids:
            dumped = instance_id.dump(include_instance_type=True)
            output.append(dumped)

        return output

    @staticmethod
    def _prepare_tracked(
        items: list[DataItem[InstanceDefinitionId]],
    ) -> list[DataItem[dict[str, Any]]]:
        """Prepare instance IDs for deletion while preserving tracking IDs."""
        output: list[DataItem[dict[str, Any]]] = []
        for item in items:
            dumped = item.item.dump(include_instance_type=True)
            output.append(DataItem(tracking_id=item.tracking_id, item=dumped))
        return output

    def _unlink_prepare(
        self,
        instance_ids: Sequence[InstanceDefinitionId],
        client: ToolkitClient,
        dry_run: bool,
        console: Console,
        verbose: bool = False,
    ) -> list[dict[str, JsonVal]]:
        self._unlink_timeseries(instance_ids, client, dry_run, console, verbose)
        self._unlink_files(instance_ids, client, dry_run, console, verbose)
        return self._prepare(instance_ids)

    def _unlink_prepare_tracked(
        self,
        items: list[DataItem[InstanceDefinitionId]],
        client: ToolkitClient,
        dry_run: bool,
        console: Console,
        logger: FileWithAggregationLogger,
        verbose: bool = False,
    ) -> list[DataItem[dict[str, Any]]]:
        """Unlink timeseries and files while preserving tracking IDs."""
        instance_ids = [item.item for item in items]
        self._unlink_timeseries(instance_ids, client, dry_run, console, verbose)
        self._unlink_files(instance_ids, client, dry_run, console, verbose)
        return self._prepare_tracked(items)

    @staticmethod
    def _delete_instance_ids(
        items: list[dict[str, JsonVal]], dry_run: bool, delete_client: HTTPClient, results: DeleteResults
    ) -> None:
        if dry_run:
            results.deleted += len(items)
            return

        responses = delete_client.request_items_retries(
            ItemsRequest(
                endpoint_url=delete_client.config.create_api_url("/models/instances/delete"),
                method="POST",
                items=[InstanceDefinitionId._load(item) for item in items],
            )
        )
        for response in responses:
            if isinstance(response, ItemsSuccessResponse):
                results.deleted += len(response.ids)
            else:
                results.failed += len(response.ids)

    @staticmethod
    def _delete_instance_ids_tracked(
        items: list[DataItem[dict[str, Any]]],
        dry_run: bool,
        delete_client: HTTPClient,
        results: DeleteResults,
        logger: FileWithAggregationLogger,
    ) -> None:
        """Delete instances while logging success/failure for each item."""
        if dry_run:
            results.deleted += len(items)
            return

        # Build a mapping from instance ID string to tracking ID for logging
        tracking_by_instance: dict[str, str] = {}
        request_items: list[InstanceDefinitionId] = []
        for item in items:
            instance_id = InstanceDefinitionId._load(item.item)
            tracking_by_instance[str(instance_id)] = item.tracking_id
            request_items.append(instance_id)

        responses = delete_client.request_items_retries(
            ItemsRequest(
                endpoint_url=delete_client.config.create_api_url("/models/instances/delete"),
                method="POST",
                items=request_items,
            )
        )
        for response in responses:
            if isinstance(response, ItemsSuccessResponse):
                results.deleted += len(response.ids)
                # Success items don't need logging - they remain without log entries (= success/pending)
            elif isinstance(response, ItemsFailedResponse):
                results.failed += len(response.ids)
                # Log failures for each failed item
                for failed_id in response.ids:
                    tracking_id = tracking_by_instance.get(failed_id, failed_id)
                    logger.log(
                        LogEntryV2(
                            id=tracking_id,
                            label="Delete failed",
                            severity=Severity.failure,
                            message=response.error_message,
                        )
                    )
            else:
                # Other failure types
                results.failed += len(response.ids)
                for failed_id in response.ids:
                    tracking_id = tracking_by_instance.get(failed_id, failed_id)
                    logger.log(
                        LogEntryV2(
                            id=tracking_id,
                            label="Delete failed",
                            severity=Severity.failure,
                            message="Unknown error during deletion",
                        )
                    )

    @staticmethod
    def _unlink_timeseries(
        instances: Sequence[InstanceDefinitionId], client: ToolkitClient, dry_run: bool, console: Console, verbose: bool
    ) -> list[InstanceDefinitionId]:
        node_ids = [instance for instance in instances if isinstance(instance, NodeId)]
        if node_ids:
            timeseries = client.tool.timeseries.retrieve(
                [InstanceId(instance_id=NodeId(space=node.space, external_id=node.external_id)) for node in node_ids],
                ignore_unknown_ids=True,
            )
            migrated_timeseries_ids = [
                InternalId(id=ts.id) for ts in timeseries if ts.instance_id and ts.pending_instance_id
            ]
            if not dry_run and timeseries:
                client.tool.timeseries.unlink_instance_ids(migrated_timeseries_ids)
                if verbose:
                    console.print(f"Unlinked {len(migrated_timeseries_ids)} timeseries from datapoints.")
            elif verbose and migrated_timeseries_ids:
                console.print(f"Would have unlinked {len(timeseries)} timeseries from datapoints.")
        return list(instances)

    @staticmethod
    def _unlink_files(
        instances: Sequence[InstanceDefinitionId], client: ToolkitClient, dry_run: bool, console: Console, verbose: bool
    ) -> list[InstanceDefinitionId]:
        file_ids = [instance for instance in instances if isinstance(instance, NodeId)]
        if file_ids:
            files = client.tool.filemetadata.retrieve(
                [InstanceId(instance_id=NodeId(space=node.space, external_id=node.external_id)) for node in file_ids],
                ignore_unknown_ids=True,
            )
            migrated_file_ids = [
                InternalId(id=file.id) for file in files if file.instance_id and file.pending_instance_id
            ]
            if not dry_run and files:
                client.tool.filemetadata.unlink_instance_ids(migrated_file_ids)
                if verbose:
                    console.print(f"Unlinked {len(migrated_file_ids)} files from nodes.")
            elif verbose and files:
                console.print(f"Would have unlinked {len(migrated_file_ids)} files from their blob content.")
        return list(instances)
