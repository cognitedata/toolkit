from collections.abc import Iterable, Mapping, Sequence
from types import MappingProxyType
from typing import ClassVar, cast

from cognite.client.data_classes.aggregations import Count

from cognite_toolkit._cdf_tk import constants
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerReference,
    EdgeRequest,
    InstanceRequest,
    InstanceResponse,
    NodeRequest,
    SpaceReference,
    ViewReference,
)
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedEdgeIdentifier,
    TypedInstanceIdentifier,
    TypedNodeIdentifier,
    TypedViewReference,
)
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, SpaceCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import StorageIOConfig
from ._base import ConfigurableStorageIO, Page, UploadableStorageIO
from .selectors import InstanceFileSelector, InstanceSelector, InstanceSpaceSelector, InstanceViewSelector


class InstanceIO(
    ConfigurableStorageIO[InstanceSelector, InstanceResponse],
    UploadableStorageIO[InstanceSelector, InstanceResponse, InstanceRequest],
):
    """This class provides functionality to interact with instances in Cognite Data Fusion (CDF).

    It is used to download, upload, and purge instances, as well as spaces,views, and containers related to instances.

    Args:
        client (ToolkitClient): An instance of ToolkitClient to interact with the CDF API.
        remove_existing_version (bool): Whether to remove existing versions of instances during upload.
            Default is True. Existing versions are used to safeguard against accidental overwrites.

    """

    KIND = "Instances"
    DISPLAY_NAME = "Instances"
    SUPPORTED_DOWNLOAD_FORMATS = frozenset({".parquet", ".csv", ".ndjson"})
    SUPPORTED_COMPRESSIONS = frozenset({".gz"})
    SUPPORTED_READ_FORMATS = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    CHUNK_SIZE = 1000
    UPLOAD_ENDPOINT = "/models/instances"
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = MappingProxyType({"autoCreateDirectRelations": True})
    BASE_SELECTOR = InstanceSelector

    def __init__(self, client: ToolkitClient, remove_existing_version: bool = True) -> None:
        super().__init__(client)
        self._remove_existing_version = remove_existing_version
        # Cache for view to read-only properties mapping
        self._view_readonly_properties_cache: dict[ViewReference, set[str]] = {}
        self._view_crud = ViewCRUD.create_loader(self.client)

    def as_id(self, item: InstanceResponse) -> str:
        return f"{item.space}:{item.external_id}"

    @staticmethod
    def _build_instance_filter(selector: InstanceViewSelector | InstanceSpaceSelector) -> InstanceFilter:
        """Build an InstanceFilter from a selector.

        Args:
            selector: The selector to build the filter from.

        Returns:
            An InstanceFilter for the toolkit instances API.
        """
        source: TypedViewReference | None = None
        space: list[str] | None = None

        if isinstance(selector, InstanceViewSelector):
            source = TypedViewReference(
                space=selector.view.space,
                external_id=selector.view.external_id,
                version=selector.view.version or "",
            )
            if selector.instance_spaces:
                space = list(selector.instance_spaces)
        elif isinstance(selector, InstanceSpaceSelector):
            space = [selector.instance_space]
            if selector.view and selector.view.version:
                source = TypedViewReference(
                    space=selector.view.space,
                    external_id=selector.view.external_id,
                    version=selector.view.version,
                )

        return InstanceFilter(
            instance_type=selector.instance_type,
            source=source,
            space=space,
        )

    def _filter_readonly_properties(self, instance: NodeRequest | EdgeRequest) -> None:
        """Filter out read-only properties from the instance.

        Args:
            instance: The instance to filter readonly properties from.
        """
        if not instance.sources:
            return

        for source in instance.sources:
            if source.properties is None:
                continue
            readonly_properties: set[str] = set()
            if isinstance(source.source, ViewReference):
                if source.source not in self._view_readonly_properties_cache:
                    self._view_readonly_properties_cache[source.source] = self._view_crud.get_readonly_properties(
                        source.source
                    )
                readonly_properties = self._view_readonly_properties_cache[source.source]
            elif isinstance(source.source, ContainerReference):
                if source.source.as_tuple() in constants.READONLY_CONTAINER_PROPERTIES:
                    readonly_properties = constants.READONLY_CONTAINER_PROPERTIES[source.source.as_tuple()]

            source.properties = {k: v for k, v in source.properties.items() if k not in readonly_properties}

    def stream_data(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[Page]:
        if isinstance(selector, InstanceViewSelector | InstanceSpaceSelector):
            instance_filter = self._build_instance_filter(selector)
            total = 0
            for page in self.client.tool.instances.iterate(instance_filter, limit=self.CHUNK_SIZE):
                if limit is not None:
                    remaining = limit - total
                    if remaining <= 0:
                        break
                    page = page[:remaining]
                total += len(page)
                if page:
                    yield Page(worker_id="main", items=page)
        elif isinstance(selector, InstanceFileSelector):
            node_ids = [TypedNodeIdentifier(space=nid.space, external_id=nid.external_id) for nid in selector.node_ids]
            for chunk in chunker_sequence(node_ids, self.CHUNK_SIZE):
                yield Page(worker_id="main", items=self.client.tool.instances.retrieve(chunk))
            edge_ids = [TypedEdgeIdentifier(space=eid.space, external_id=eid.external_id) for eid in selector.edge_ids]
            for chunk in chunker_sequence(edge_ids, self.CHUNK_SIZE):
                yield Page(worker_id="main", items=self.client.tool.instances.retrieve(chunk))
        else:
            raise NotImplementedError()

    def download_ids(
        self, selector: InstanceSelector, limit: int | None = None
    ) -> Iterable[list[TypedInstanceIdentifier]]:
        if isinstance(selector, InstanceFileSelector) and selector.validate_instance is False:
            typed_ids: list[TypedInstanceIdentifier] = [
                TypedNodeIdentifier(space=item.space, external_id=item.external_id)
                if item.instance_type == "node"
                else TypedEdgeIdentifier(space=item.space, external_id=item.external_id)
                for item in selector.items
            ]
            if limit is not None:
                typed_ids = typed_ids[:limit]
            yield from chunker_sequence(typed_ids, self.CHUNK_SIZE)
        else:
            yield from ([instance.as_id() for instance in chunk.items] for chunk in self.stream_data(selector, limit))

    def count(self, selector: InstanceSelector) -> int | None:
        if isinstance(selector, InstanceViewSelector) or (
            isinstance(selector, InstanceSpaceSelector) and selector.view
        ):
            result = self.client.data_modeling.instances.aggregate(
                # MyPy do not understand that selector.view is always defined here.
                view=selector.view.as_id(),  # type: ignore[union-attr]
                aggregates=Count("externalId"),
                instance_type=selector.instance_type,
                space=selector.get_instance_spaces(),
            )
            return int(result.value or 0)
        elif isinstance(selector, InstanceSpaceSelector):
            statistics = self.client.data_modeling.statistics.spaces.retrieve(space=selector.instance_space)
            if statistics is None:
                return None
            if selector.instance_type == "node":
                return statistics.nodes
            elif selector.instance_type == "edge":
                return statistics.edges
            # This should never happen due to validation in the selector.
            raise ValueError(f"Unknown instance type {selector.instance_type!r}")
        elif isinstance(selector, InstanceFileSelector):
            return len(selector.instance_ids)
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Sequence[InstanceResponse], selector: InstanceSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        return [instance.as_request_resource().dump() for instance in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> NodeRequest | EdgeRequest:
        instance_type = item_json.get("instanceType")
        item_to_load = dict(item_json)
        if self._remove_existing_version and "existingVersion" in item_to_load:
            del item_to_load["existingVersion"]
        instance: NodeRequest | EdgeRequest
        if instance_type == "node":
            instance = NodeRequest.model_validate(item_to_load)
        elif instance_type == "edge":
            instance = EdgeRequest.model_validate(item_to_load)
        else:
            raise ValueError(f"Unknown instance type {instance_type!r}")
        self._filter_readonly_properties(instance)
        return instance

    def configurations(self, selector: InstanceSelector) -> Iterable[StorageIOConfig]:
        if not isinstance(selector, InstanceViewSelector | InstanceSpaceSelector):
            return
        spaces = list(set((selector.get_instance_spaces() or []) + (selector.get_schema_spaces() or [])))
        if not spaces:
            return
        space_crud = SpaceCRUD.create_loader(self.client)
        retrieved_spaces = space_crud.retrieve([SpaceReference(space=space) for space in spaces])
        retrieved_spaces = [space for space in retrieved_spaces if not space.is_global]
        if not retrieved_spaces:
            return
        for space in retrieved_spaces:
            yield StorageIOConfig(
                kind=SpaceCRUD.kind,
                folder_name=SpaceCRUD.folder_name,
                value=space_crud.dump_resource(space),
                filename=sanitize_filename(space.space),
            )
        if not selector.view:
            return
        view_ref = ViewReference(
            space=selector.view.space,
            external_id=selector.view.external_id,
            version=cast(str, selector.view.version),
        )
        view_crud = ViewCRUD(self.client, None, None, topological_sort_implements=True)
        views = view_crud.retrieve([view_ref])
        views = [view for view in views if not view.is_global]
        if not views:
            return
        for view in views:
            filename = f"{view.space}_{view.external_id}"
            if view.version is not None:
                filename += f"_{view.version}"
            yield StorageIOConfig(
                kind=ViewCRUD.kind,
                folder_name=ViewCRUD.folder_name,
                value=view_crud.dump_resource(view),
                filename=sanitize_filename(filename),
            )
        container_ids = list({container for view in views for container in view.mapped_containers})
        if not container_ids:
            return
        container_crud = ContainerCRUD.create_loader(self.client)
        containers = container_crud.retrieve(container_ids)
        containers = [container for container in containers if not container.is_global]
        if not containers:
            return
        for container in containers:
            yield StorageIOConfig(
                kind=ContainerCRUD.kind,
                folder_name=ContainerCRUD.folder_name,
                value=container_crud.dump_resource(container),
                filename=sanitize_filename(f"{container.space}_{container.external_id}"),
            )
