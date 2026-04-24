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
from cognite.client.data_classes.data_modeling.statistics import InstanceStatistics, SpaceStatistics
from cognite.client.exceptions import CogniteAPIError
from pydantic import JsonValue
from rich import print
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import RequestItem
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsRequest,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.identifiers import (
    ContainerId,
    InstanceDefinitionId,
    InstanceId,
    InternalId,
    ViewId,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import ContainerFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeId, NodeResponse, SpaceId
from cognite_toolkit._cdf_tk.constants import DMS_SOFT_DELETED_INSTANCE_LIMIT_MARGIN
from cognite_toolkit._cdf_tk.data_classes import DeployResults, ResourceDeployResult
from cognite_toolkit._cdf_tk.dataio import InstanceIO, Page
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
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
from cognite_toolkit._cdf_tk.utils.validate_access import ValidateAccess

from ..dataio.logger import FileWithAggregationLogger, ItemsResult, LogEntryV2, Severity, display_item_results
from ..utils.fileio import NDJsonWriter, Uncompressed
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
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[list[ResourceResponseProtocol]], list[JsonVal]]:
        raise NotImplementedError()

    def get_extra_fields(self) -> dict[str, JsonVal]:
        return {}


@dataclass
class DataModelingToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[list[ResourceResponseProtocol]], list[JsonVal]]:
        def as_id(chunk: list[ResourceResponseProtocol]) -> list[JsonVal]:
            # We know that all data modeling resources implement as_id
            return [item.as_id().dump() for item in chunk]  # type: ignore[attr-defined]

        return as_id


@dataclass
class EdgeToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[list[ResourceResponseProtocol]], list[JsonVal]]:
        def as_id(chunk: list[ResourceResponseProtocol]) -> list[JsonVal]:
            return [
                {"space": item.space, "externalId": item.external_id, "instanceType": "edge"}
                for item in cast(list[Edge], chunk)
            ]

        return as_id


@dataclass
class NodesToDelete(ToDelete):
    delete_datapoints: bool
    delete_file_content: bool

    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[list[ResourceResponseProtocol]], list[JsonVal]]:
        def check_for_data(chunk: list[NodeResponse]) -> list[JsonVal]:
            instance_ids = [InstanceId(instance_id=item.as_id()) for item in chunk]
            found_ids: set[NodeId] = set()
            if not self.delete_datapoints:
                timeseries = client.tool.timeseries.retrieve(instance_ids, ignore_unknown_ids=True)
                found_ids |= {ts.instance_id for ts in timeseries if ts.instance_id is not None}
            if not self.delete_file_content:
                files = client.tool.filemetadata.retrieve(instance_ids, ignore_unknown_ids=True)
                found_ids |= {f.instance_id for f in files if f.instance_id is not None}
            if found_ids and verbose:
                console.print(f"Skipping {found_ids} nodes as they have datapoints or file content")
            process_results.unchanged += len(found_ids)
            result: list[JsonVal] = []
            for node_id in (n.instance_id for n in instance_ids if n.instance_id not in found_ids):
                dumped = node_id.dump(include_instance_type=True)
                result.append(dumped)
            return result

        return check_for_data  # type: ignore[return-value]


@dataclass
class IdResourceToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[list[ResourceResponseProtocol]], list[JsonVal]]:
        def as_id(chunk: list[ResourceResponseProtocol]) -> list[JsonVal]:
            # We know that all id resources have an id attribute
            return [{"id": item.id} for item in chunk]  # type: ignore[attr-defined]

        return as_id


@dataclass
class ExternalIdToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[list[ResourceResponseProtocol]], list[JsonVal]]:
        def as_external_id(chunk: list[ResourceResponseProtocol]) -> list[JsonVal]:
            # We know that all external id resources have an external_id attribute
            return [{"externalId": item.external_id} for item in chunk]  # type: ignore[attr-defined]

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

        if stats.containers > 0:
            self._block_if_external_views_reference_containers(client, selected_space)

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
            results = self._delete_resources(to_delete, client, verbose, selected_space, None)
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
        except CogniteAPIError as e:
            self.warn(HighSeverityWarning(f"Failed to delete space {selected_space!r}: {e}"))
        else:
            results[space_loader.display_name] = ResourceDeployResult(space_loader.display_name, deleted=1)

    def _delete_resources(
        self,
        to_delete: list[ToDelete],
        client: ToolkitClient,
        verbose: bool,
        space: str | None,
        data_set_external_id: str | None,
    ) -> DeployResults:
        results = DeployResults([], "purge", dry_run=False)
        console = Console()
        with HTTPClient(client.config, max_retries=10) as delete_client:
            for item in to_delete:
                if item.total == 0:
                    results[item.display_name] = ResourceDeployResult(item.display_name, deleted=0)
                    continue
                # Two results objects since they are updated concurrently
                process_results = ResourceDeployResult(item.display_name)
                write_results = ResourceDeployResult(item.display_name)
                executor = ProducerWorkerExecutor[list[ResourceResponseProtocol], list[JsonVal]](
                    download_iterable=self._iterate_batch(
                        item.crud, space, data_set_external_id, batch_size=self.BATCH_SIZE_DM
                    ),
                    process=item.get_process_function(client, console, verbose, process_results),
                    write=self._purge_batch(item, item.delete_url, delete_client, write_results),
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
                if executor.error_occurred:
                    if verbose and executor.error_traceback:
                        executor.print_traceback()
                    self.warn(
                        HighSeverityWarning(f"Failed to delete all {item.display_name}. {executor.error_message}")
                    )
        return results

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
            results = self._delete_resources(to_delete, client, verbose, None, selected_data_set_external_id)
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
    def _block_if_external_views_reference_containers(client: ToolkitClient, selected_space: str) -> None:
        """Block the purge if any container in the space is referenced by views in other spaces.

        Uses the ``models/containers/inspect`` endpoint with both the ``involvedViews`` and
        ``totalInvolvedViewCount`` operations.  The total count is authoritative — it includes views
        the caller cannot access — so blocking is decided on the count, not the view list.
        Same-space views are excluded because they are themselves part of the purge.
        """
        container_ids = [
            ContainerId(space=container.space, external_id=container.external_id)
            for container in client.tool.containers.list(filter=ContainerFilter(space=selected_space))
        ]
        if not container_ids:
            return

        # (external_views_seen, hidden_count) per blocked container
        blocked: dict[ContainerId, tuple[list[ViewId], int]] = {}
        for inspected in client.tool.containers.inspect(container_ids):
            results = inspected.inspection_results
            external_seen = [view for view in results.involved_views if view.space != selected_space]
            # Hidden views (not returned in involvedViews) must be from other spaces: purge access
            # to selected_space implies read access, so any same-space views would be visible.
            hidden_count = results.involved_view_count - len(results.involved_views)
            if not external_seen and hidden_count == 0:
                continue
            container_id = ContainerId(space=inspected.space, external_id=inspected.external_id)
            blocked[container_id] = (external_seen, hidden_count)
        if not blocked:
            return

        table = Table(
            title=f"Purge is blocked since containers in [bold]{selected_space}[/bold] are referenced by views in other spaces",
            title_justify="left",
            show_lines=True,
        )
        table.add_column("Container", no_wrap=True)
        table.add_column("Referencing view(s)", no_wrap=True)
        for container_id, (external_seen, hidden_count) in blocked.items():
            container_label = f"{container_id.space}:{container_id.external_id}"
            rows: list[tuple[str, str]] = [
                (container_label if index == 0 else "", f"{view.space}:{view.external_id}/{view.version}")
                for index, view in enumerate(external_seen)
            ]
            if hidden_count > 0:
                hidden_label = f"[dim italic]{hidden_count} view(s) you do not have access to[/]"
                rows.append(("" if external_seen else container_label, hidden_label))
            for label, view_label in rows:
                table.add_row(label, view_label)
        print(table)
        raise ToolkitValueError(
            f"Cannot proceed with purge of space {selected_space!r}: one or more containers in this space are referenced by views "
            "in other spaces. Deleting containers that are still referenced by views would cause breaking changes to those views. "
            "If you are sure you still want to delete these containers, you need to remove all versions of all views that reference these containers (refer to the table above), then re-run the purge."
        )

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

        log_filestem = f"purge_{date.today().strftime('%Y%m%d')}"
        with (
            HTTPClient(config=client.config) as delete_client,
            NDJsonWriter(
                log_dir, kind="PurgeLogs", default_filestem=log_filestem, compression=Uncompressed
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
                write=partial(self._delete_instance_ids, dry_run=dry_run, delete_client=delete_client),
                total_item_count=total,
                max_queue_size=10,
                download_description=f"Retrieving instances from {selector!s}",
                process_description=f"{process_str} instances from files/timeseries" if unlink else "",
                write_description=f"{write_str} instances",
                console=console,
            )
            executor.run()
            items_results = logger.finalize(is_dry_run=False)
            display_item_results(items_results, title=f"Finished {selector.display_name}", console=console)

            executor.raise_on_error()

        return DeleteResults.from_item_results(items_results)

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
        items: Page[InstanceDefinitionId], dry_run: bool, delete_client: HTTPClient, logger: FileWithAggregationLogger,
    ) -> None:
        if dry_run:
            ...
            return

        responses = delete_client.request_items_retries(
            ItemsRequest(
                endpoint_url=delete_client.config.create_api_url("/models/instances/delete"),
                method="POST",
                items=items,
            )
        )
        for response in responses:
            if not isinstance(response, ItemsSuccessResponse):


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
                InternalId(id=ts.id): ts.instance_id for ts in timeseries if ts.instance_id and ts.pending_instance_id
            }
            if not dry_run and timeseries:
                client.tool.timeseries.unlink_instance_ids(list(migrated_timeseries_ids.keys()))
                for value in migrated_timeseries_ids.values():
                    data_item = node_items[value]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Unlinked timeseries",
                            severity=Severity.info,
                            message=f"Unlinked TimeSeries with internal ID {value.id} from Node {data_item.item!r}",
                        )
                    )
            elif migrated_timeseries_ids:
                for value in migrated_timeseries_ids.values():
                    data_item = node_items[value]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Ready to unlink timeseries",
                            severity=Severity.info,
                            message=f"Ready to unlink TimeSeries with internal ID {value.id} from Node {data_item.item!r}",
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
                InternalId(id=file.id): file.instance_id for file in files if file.instance_id and file.pending_instance_id
            }
            if not dry_run and files:
                client.tool.filemetadata.unlink_instance_ids(list(migrated_file_ids.keys()))
                for value in migrated_file_ids.values():
                    data_item = node_items[value]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Unlinked file",
                            severity=Severity.info,
                            message=f"Unlinked File with internal ID {value.id} from Node {data_item.item!r}",
                        )
                    )
            elif migrated_file_ids:
                for value in migrated_file_ids.values():
                    data_item = node_items[value]
                    logger.log(
                        LogEntryV2(
                            id=data_item.tracking_id,
                            label="Ready to unlink file",
                            severity=Severity.info,
                            message=f"Ready to unlink File with internal ID {value.id} from Node {data_item.item!r}",
                        )
                    )
        return None
