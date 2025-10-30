import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Hashable, Iterable, Sequence
from dataclasses import dataclass
from functools import partial
from typing import Literal, cast

import questionary
from cognite.client.data_classes import DataSetUpdate
from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling import (
    EdgeList,
    NodeId,
    NodeList,
    ViewId,
)
from cognite.client.data_classes.data_modeling.statistics import SpaceStatistics
from cognite.client.exceptions import CogniteAPIError
from cognite.client.utils._identifier import InstanceId
from rich import print
from rich.console import Console
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.cruds import (
    AssetCRUD,
    ContainerCRUD,
    DataModelCRUD,
    EdgeCRUD,
    EventCRUD,
    ExtractionPipelineCRUD,
    FileMetadataCRUD,
    LabelCRUD,
    NodeCRUD,
    RelationshipCRUD,
    ResourceCRUD,
    SequenceCRUD,
    SpaceCRUD,
    ThreeDModelCRUD,
    TimeSeriesCRUD,
    TransformationCRUD,
    ViewCRUD,
    WorkflowCRUD,
)
from cognite_toolkit._cdf_tk.data_classes import DeployResults, ResourceDeployResult
from cognite_toolkit._cdf_tk.exceptions import (
    AuthorizationError,
    ToolkitMissingResourceError,
    ToolkitValueError,
)
from cognite_toolkit._cdf_tk.storageio import InstanceIO
from cognite_toolkit._cdf_tk.storageio.selectors import InstanceSelector
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
from cognite_toolkit._cdf_tk.utils.http_client import (
    FailedRequestItems,
    FailedResponseItems,
    HTTPClient,
    ItemsRequest,
    SuccessResponseItems,
)
from cognite_toolkit._cdf_tk.utils.producer_worker import ProducerWorkerExecutor
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
from cognite_toolkit._cdf_tk.utils.validate_access import ValidateAccess

from ._base import ToolkitCommand


@dataclass
class DeleteResults:
    deleted: int = 0
    failed: int = 0


@dataclass
class DeleteItem:
    item: JsonVal
    as_id_fun: Callable[[JsonVal], Hashable]

    def dump(self) -> JsonVal:
        return self.item

    def as_id(self) -> Hashable:
        return self.as_id_fun(self.item)


@dataclass
class ToDelete(ABC):
    crud: ResourceCRUD
    total: int
    delete_url: str

    @property
    def display_name(self) -> str:
        return self.crud.display_name

    @abstractmethod
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[CogniteResourceList], list[JsonVal]]:
        raise NotImplementedError()

    def get_extra_fields(self) -> dict[str, JsonVal]:
        return {}


@dataclass
class DataModelingToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[CogniteResourceList], list[JsonVal]]:
        def as_id(chunk: CogniteResourceList) -> list[JsonVal]:
            return [item.as_id().dump(include_type=False) for item in chunk]

        return as_id


@dataclass
class EdgeToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[CogniteResourceList], list[JsonVal]]:
        def as_id(chunk: CogniteResourceList) -> list[JsonVal]:
            return [
                {"space": item.space, "externalId": item.external_id, "instanceType": "edge"}
                for item in cast(EdgeList, chunk)
            ]

        return as_id


@dataclass
class NodesToDelete(ToDelete):
    delete_datapoints: bool
    delete_file_content: bool

    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[CogniteResourceList], list[JsonVal]]:
        def check_for_data(chunk: CogniteResourceList) -> list[JsonVal]:
            node_ids = cast(NodeList, chunk).as_ids()
            found_ids: set[InstanceId] = set()
            if not self.delete_datapoints:
                timeseries = client.time_series.retrieve_multiple(instance_ids=node_ids, ignore_unknown_ids=True)
                found_ids |= {ts.instance_id for ts in timeseries if ts.instance_id is not None}
            if not self.delete_file_content:
                files = client.files.retrieve_multiple(instance_ids=node_ids, ignore_unknown_ids=True)
                found_ids |= {f.instance_id for f in files if f.instance_id is not None}
            if found_ids and verbose:
                console.print(f"Skipping {found_ids} nodes as they have datapoints or file content")
            process_results.unchanged += len(found_ids)
            result: list[JsonVal] = []
            for node_id in (n for n in node_ids if n not in found_ids):
                dumped = node_id.dump(include_instance_type=True)
                # The delete endpoint expects "instanceType" instead of "type"
                dumped["instanceType"] = dumped.pop("type")
                # MyPy think complains about invariant here, even though dict[str, str] is a type of JsonVal
                result.append(dumped)  # type: ignore[arg-type]
            return result

        return check_for_data


@dataclass
class IdResourceToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[CogniteResourceList], list[JsonVal]]:
        def as_id(chunk: CogniteResourceList) -> list[JsonVal]:
            return [{"id": item.id} for item in chunk]

        return as_id


@dataclass
class ExternalIdToDelete(ToDelete):
    def get_process_function(
        self, client: ToolkitClient, console: Console, verbose: bool, process_results: ResourceDeployResult
    ) -> Callable[[CogniteResourceList], list[JsonVal]]:
        def as_external_id(chunk: CogniteResourceList) -> list[JsonVal]:
            return [{"externalId": item.external_id} for item in chunk]

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
        auto_yes: bool = False,
        verbose: bool = False,
    ) -> DeployResults:
        # Warning Messages
        if not dry_run:
            self._print_panel("space", selected_space)

        if not dry_run and not auto_yes:
            confirm = questionary.confirm(
                f"Are you really sure you want to purge the {selected_space!r} space?", default=False
            ).ask()
            if not confirm:
                return DeployResults([], "purge", dry_run=dry_run)

        stats = client.data_modeling.statistics.spaces.retrieve(selected_space)
        if stats is None:
            raise ToolkitMissingResourceError(f"Space {selected_space!r} does not exist")

        # ValidateAuth
        validator = ValidateAccess(client, "purge")
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

    def _create_to_delete_list_purge_space(
        self, client: ToolkitClient, delete_datapoints: bool, delete_file_content: bool, stats: SpaceStatistics
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
                DataModelCRUD.create_loader(client),
                stats.data_models,
                config.create_api_url("/models/datamodels/delete"),
            ),
            DataModelingToDelete(
                ViewCRUD.create_loader(client), stats.views, config.create_api_url("/models/views/delete")
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
            space_loader.delete([selected_space])
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
                iteration_count: int | None = None
                if item.total > 0:
                    iteration_count = item.total // self.BATCH_SIZE_DM + (
                        1 if item.total % self.BATCH_SIZE_DM > 0 else 0
                    )
                executor = ProducerWorkerExecutor[CogniteResourceList, list[JsonVal]](
                    download_iterable=self._iterate_batch(
                        item.crud, space, data_set_external_id, batch_size=self.BATCH_SIZE_DM
                    ),
                    process=item.get_process_function(client, console, verbose, process_results),
                    write=self._purge_batch(item, item.delete_url, delete_client, write_results),
                    max_queue_size=10,
                    iteration_count=iteration_count,
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
        crud: ResourceCRUD, selected_space: str | None, data_set_external_id: str | None, batch_size: int
    ) -> Iterable[CogniteResourceList]:
        batch = crud.list_cls([])
        for resource in crud.iterate(space=selected_space, data_set_external_id=data_set_external_id):
            batch.append(resource)
            if len(batch) >= batch_size:
                yield batch
                batch = crud.list_cls([])
        if batch:
            yield batch

    @staticmethod
    def _purge_batch(
        delete_item: ToDelete, delete_url: str, delete_client: HTTPClient, result: ResourceDeployResult
    ) -> Callable[[list[JsonVal]], None]:
        crud = delete_item.crud

        def as_id(item: JsonVal) -> Hashable:
            try:
                return crud.get_id(item)
            except KeyError:
                # Fallback to internal ID
                return crud.get_internal_id(item)

        def process(items: list[JsonVal]) -> None:
            responses = delete_client.request_with_retries(
                ItemsRequest(
                    endpoint_url=delete_url,
                    method="POST",
                    items=[DeleteItem(item=item, as_id_fun=as_id) for item in items],
                    extra_body_fields=delete_item.get_extra_fields(),
                )
            )
            for response in responses:
                if isinstance(response, SuccessResponseItems):
                    result.deleted += len(response.ids)
                else:
                    result.unchanged += len(items)

        return process

    def dataset_v2(
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
            confirm = questionary.confirm(
                f"Are you really sure you want to purge the {selected_data_set_external_id!r} dataSet?", default=False
            ).ask()
            if not confirm:
                return DeployResults([], "purge", dry_run=dry_run)

        # Validate Auth
        validator = ValidateAccess(client, "purge")
        data_set_id = client.lookup.data_sets.id(selected_data_set_external_id)
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
                        RelationshipCRUD.create_loader(client),
                        RelationshipAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/relationships/delete"),
                    ),
                    IdResourceToDelete(
                        EventCRUD.create_loader(client),
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
                        SequenceCRUD.create_loader(client),
                        SequenceAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/sequences/delete"),
                    ),
                    IdResourceToDelete(
                        three_d_crud,
                        sum(1 for _ in three_d_crud.iterate(data_set_external_id=data_set_external_id)),
                        config.create_api_url("/3d/models/delete"),
                    ),
                    AssetToDelete(
                        AssetCRUD.create_loader(client),
                        AssetAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/assets/delete"),
                        recursive=asset_recursive,
                    ),
                    ExternalIdToDelete(
                        LabelCRUD.create_loader(client),
                        LabelCountAggregator(client).count(data_set_external_id=data_set_external_id),
                        config.create_api_url("/labels/delete"),
                    ),
                ]
            )
        if include_configurations:
            transformation_crud = TransformationCRUD.create_loader(client)
            workflow_crud = WorkflowCRUD.create_loader(client)
            extraction_pipeline_crud = ExtractionPipelineCRUD.create_loader(client)

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
        dry_run: bool = False,
        auto_yes: bool = False,
        unlink: bool = True,
        verbose: bool = False,
    ) -> DeleteResults:
        """Purge instances"""
        io = InstanceIO(client)
        console = Console()
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
            self._print_panel("instances", str(selector))
            if not auto_yes:
                confirm = questionary.confirm(
                    f"Are you really sure you want to purge all {total:,} instances in {selector!s}?",
                    default=False,
                ).ask()
                if not confirm:
                    return DeleteResults()

        process: Callable[[list[InstanceId]], list[dict[str, JsonVal]]] = self._prepare
        if unlink:
            process = partial(self._unlink_prepare, client=client, dry_run=dry_run, console=console, verbose=verbose)

        iteration_count = int(total // io.CHUNK_SIZE + (1 if total % io.CHUNK_SIZE > 0 else 0))
        with HTTPClient(config=client.config) as delete_client:
            process_str = "Would be unlinking" if dry_run else "Unlinking"
            write_str = "Would be deleting" if dry_run else "Deleting"
            results = DeleteResults()
            executor = ProducerWorkerExecutor[list[InstanceId], list[dict[str, JsonVal]]](
                download_iterable=io.download_ids(selector),
                process=process,
                write=partial(self._delete_instance_ids, dry_run=dry_run, delete_client=delete_client, results=results),
                iteration_count=iteration_count,
                max_queue_size=10,
                download_description=f"Retrieving instances from {selector!s}",
                process_description=f"{process_str} instances from files/timeseries" if unlink else "",
                write_description=f"{write_str} instances",
                console=console,
            )

            executor.run()
            executor.raise_on_error()

        prefix = "Would have purged" if dry_run else "Purged"
        if results.failed == 0:
            console.print(f"{prefix} {results.deleted:,} instances in {selector!s}")
        else:
            console.print(
                f"{prefix} {results.deleted:,} instances in {selector!s}, but failed to purge {results.failed:,} instances"
            )
        return results

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
    def _prepare(instance_ids: list[InstanceId]) -> list[dict[str, JsonVal]]:
        output: list[dict[str, JsonVal]] = []
        for instance_id in instance_ids:
            dumped = instance_id.dump(include_instance_type=True)
            # The PySDK uses 'type' instead of 'instanceType' which is required by the delete endpoint
            dumped["instanceType"] = dumped.pop("type")
            # MyPy does not understand that InstanceId.dump() returns dict[str, JsonVal]
            output.append(dumped)  # type: ignore[arg-type]

        return output

    def _unlink_prepare(
        self,
        instance_ids: list[InstanceId],
        client: ToolkitClient,
        dry_run: bool,
        console: Console,
        verbose: bool = False,
    ) -> list[dict[str, JsonVal]]:
        self._unlink_timeseries(instance_ids, client, dry_run, console, verbose)
        self._unlink_files(instance_ids, client, dry_run, console, verbose)
        return self._prepare(instance_ids)

    @staticmethod
    def _delete_instance_ids(
        items: list[dict[str, JsonVal]], dry_run: bool, delete_client: HTTPClient, results: DeleteResults
    ) -> None:
        if dry_run:
            results.deleted += len(items)
            return

        responses = delete_client.request_with_retries(
            ItemsRequest(
                delete_client.config.create_api_url("/models/instances/delete"),
                method="POST",
                # MyPy does not understand that InstanceId.load handles dict[str, JsonVal]
                items=[DeleteItem(item=item, as_id_fun=InstanceId.load) for item in items],  # type: ignore[arg-type]
            )
        )
        for response in responses:
            if isinstance(response, SuccessResponseItems):
                results.deleted += len(response.ids)
            elif isinstance(response, FailedResponseItems | FailedRequestItems):
                results.failed += len(response.ids)
            else:
                results.failed += len(items)

    @staticmethod
    def _unlink_timeseries(
        instances: list[InstanceId], client: ToolkitClient, dry_run: bool, console: Console, verbose: bool
    ) -> list[InstanceId]:
        node_ids = [instance for instance in instances if isinstance(instance, NodeId)]
        if node_ids:
            timeseries = client.time_series.retrieve_multiple(instance_ids=node_ids, ignore_unknown_ids=True)
            if not dry_run and timeseries:
                migrated_timeseries_ids = [ts.id for ts in timeseries if ts.instance_id and ts.pending_instance_id]  # type: ignore[attr-defined]
                client.time_series.unlink_instance_ids(id=migrated_timeseries_ids)
                if verbose:
                    console.print(f"Unlinked {len(migrated_timeseries_ids)} timeseries from datapoints.")
            elif verbose and timeseries:
                console.print(f"Would have unlinked {len(timeseries)} timeseries from datapoints.")
        return instances

    @staticmethod
    def _unlink_files(
        instances: list[InstanceId], client: ToolkitClient, dry_run: bool, console: Console, verbose: bool
    ) -> list[InstanceId]:
        file_ids = [instance for instance in instances if isinstance(instance, NodeId)]
        if file_ids:
            files = client.files.retrieve_multiple(instance_ids=file_ids, ignore_unknown_ids=True)
            if not dry_run and files:
                migrated_file_ids = [
                    file.id
                    for file in files
                    if file.instance_id and file.pending_instance_id and file.id is not None  # type: ignore[attr-defined]
                ]
                client.files.unlink_instance_ids(id=migrated_file_ids)
                if verbose:
                    console.print(f"Unlinked {len(migrated_file_ids)} files from nodes.")
            elif verbose and files:
                console.print(f"Would have unlinked {len(files)} files from their blob content.")
        return instances

    @staticmethod
    def get_selected_view_id(view: list[str]) -> ViewId:
        if not (isinstance(view, list) and len(view) == 3):
            raise ToolkitValueError(f"Invalid view format: {view}. Expected format is 'space externalId version'.")

        return ViewId.load(tuple(view))  # type: ignore[arg-type]
