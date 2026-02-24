from collections.abc import Iterable, Mapping, Sequence
from types import MappingProxyType
from typing import Any, ClassVar, Literal, cast

from cognite.client.data_classes.aggregations import Count
from cognite.client.data_classes.data_modeling import EdgeId, NodeId, ViewId
from cognite.client.utils._identifier import InstanceId

from cognite_toolkit._cdf_tk import constants
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerReference,
    EdgeProperty,
    InstanceRequest,
    InstanceResponse,
    QueryEdgeExpression,
    QueryEdgeTableExpression,
    QueryNodeExpression,
    QueryNodeTableExpression,
    QueryRequest,
    QueryResponse,
    QuerySelect,
    QuerySelectSource,
    QuerySortSpec,
    SpaceReference,
    ViewReference,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import InstanceRequestAdapter
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import (
    TypedEdgeIdentifier,
    TypedNodeIdentifier,
    TypedViewReference,
)
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, SpaceCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal

from . import StorageIOConfig
from ._base import ConfigurableStorageIO, Page, UploadableStorageIO
from .selectors import InstanceFileSelector, InstanceSelector, InstanceSpaceSelector, InstanceViewSelector, SelectedView


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
    def _build_list_filter(selector: InstanceViewSelector | InstanceSpaceSelector) -> InstanceFilter:
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

    @staticmethod
    def _build_query_filter(selector: InstanceViewSelector, instance_type: Literal["node", "edge"]) -> dict[str, Any]:
        """Build a filter dict for the query endpoint from an InstanceViewSelector."""
        leaf_filters: list[dict[str, Any]] = [
            {"hasData": [{**selector.view.as_id().dump(), "type": "view"}]},
        ]
        if selector.instance_spaces and len(selector.instance_spaces) == 1:
            leaf_filters.append(
                {"equals": {"property": [instance_type, "space"], "value": selector.instance_spaces[0]}}
            )
        elif selector.instance_spaces and len(selector.instance_spaces) > 1:
            leaf_filters.append(
                {"in": {"property": [instance_type, "space"], "values": list(selector.instance_spaces)}}
            )
        if len(leaf_filters) == 1:
            return leaf_filters[0]
        return {"and": leaf_filters}

    def _filter_readonly_properties(self, instance: InstanceRequest) -> None:
        """Filter out read-only properties from the instance.

        Warnings: This mutates the instance in-place.

        This is as of 17/02/26, the path, root, and pathLastUpdatedTime time in the CogniteAsset container,
        and isUploaded and uploadedTime in the CogniteFile container.
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
            if not readonly_properties:
                continue
            source.properties = {k: v for k, v in source.properties.items() if k not in readonly_properties}

    def stream_data(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[Page]:
        if isinstance(selector, InstanceViewSelector) and selector.include_edges and selector.instance_type == "node":
            yield from self._instances_with_container_and_edge_properties(selector, limit)
        elif isinstance(selector, InstanceViewSelector | InstanceSpaceSelector):
            yield from self._instances_with_container_properties(selector, limit)
        elif isinstance(selector, InstanceFileSelector):
            for chunk in chunker_sequence(selector.ids, self.CHUNK_SIZE):
                yield Page(worker_id="main", items=self.client.tool.instances.retrieve(chunk))
        else:
            raise NotImplementedError()

    def _instances_with_container_and_edge_properties(
        self, selector: InstanceViewSelector, limit: int | None
    ) -> Iterable[Page]:
        instance_filter = self._build_query_filter(selector, "node")
        views = self.client.tool.views.retrieve([selector.view.as_id()])
        if not views:
            raise ToolkitValueError(f"The view {selector.view.as_id()} does not exist")
        view = views[0]
        with_: dict[str, QueryNodeExpression | QueryEdgeExpression] = {
            "nodes": QueryNodeExpression(
                limit=min(self.CHUNK_SIZE, limit) if limit is not None else self.CHUNK_SIZE,
                nodes=QueryNodeTableExpression(filter=instance_filter),
                # Sort to ensure performance.
                sort=[QuerySortSpec(property=["node", "space"]), QuerySortSpec(property=["node", "externalId"])],
            )
        }
        select: dict[str, QuerySelect] = {
            "nodes": QuerySelect(sources=[QuerySelectSource(source=view.as_id(), properties=["*"])])
        }
        edge_ids: list[str] = []
        for prop_id, prop in view.properties.items():
            if not isinstance(prop, EdgeProperty):
                continue
            with_[prop_id] = QueryEdgeExpression(
                edges=QueryEdgeTableExpression(
                    from_="nodes",
                    chain_to="source" if prop.direction == "outwards" else "destination",
                    direction=prop.direction,
                ),
                sort=[QuerySortSpec(property=["edge", "space"]), QuerySortSpec(property=["edge", "externalId"])],
            )
            edge_ids.append(prop_id)
            select[prop_id] = QuerySelect()

        query = QueryRequest(with_=with_, select=select)
        total = 0
        while True:
            response = self._exhaust_edge_queries(query, edge_ids)
            items = response.items.get("nodes", [])
            # De-duplicate edges across properties, as the same edge can be returned for multiple
            # properties if it connects two nodes that are in the result set.
            edges: dict[TypedNodeIdentifier | TypedEdgeIdentifier, InstanceResponse] = {}
            for prop_id in edge_ids:
                for edge in response.items.get(prop_id, []):
                    ref = edge.as_id()
                    if ref not in edges:
                        edges[ref] = edge
            items.extend(edges.values())
            total += len(items)
            yield Page(worker_id="main", items=items, next_cursor=response.next_cursor.get("nodes"))
            next_cursor = response.next_cursor.get("nodes")
            if next_cursor is None or (limit is not None and total >= limit) or not items:
                break
            page_limit = min(self.CHUNK_SIZE, limit - total) if limit is not None else self.CHUNK_SIZE
            query.with_["nodes"].limit = page_limit
            query.cursors = {"nodes": next_cursor}

    def _exhaust_edge_queries(self, query: QueryRequest, edge_properties: list[str]) -> QueryResponse:
        """Exhausts the edge queries in the with_ clause of the query until all cursors are None.

        This is necessary to ensure that we get all edges for the nodes in the result set, as edges can be returned
        on multiple properties if they connect two nodes that are in the result set.

        Args:
            query: The query to execute. This will be mutated in-place.
            edge_properties: The list of edge properties to exhaust.

        Returns:
            The final QueryResponse with all edge queries exhausted.
        """
        first: QueryResponse | None = None
        while True:
            response = self.client.tool.instances.query(query)
            if first is None:
                first = response
            else:
                for key, items in response.items.items():
                    if key not in first.items:
                        first.items[key] = items
                    else:
                        first.items[key].extend(items)
            next_cursors: dict[str, str] = {}
            for prop_id in edge_properties:
                edge_cursor = response.next_cursor.get(prop_id)
                if edge_cursor is not None:
                    next_cursors[prop_id] = edge_cursor
            if not next_cursors or not response.items:
                return first
            node_cursor = response.next_cursor.get("nodes")
            if node_cursor is not None:
                next_cursors["nodes"] = node_cursor
            query = query.model_copy(update={"cursors": next_cursors})

    def _instances_with_container_properties(
        self, selector: InstanceViewSelector | InstanceSpaceSelector, limit: int | None
    ) -> Iterable[Page]:
        instance_filter = self._build_list_filter(selector)
        total = 0
        cursor: str | None = None
        while cursor is not None or total == 0:
            page_limit = min(self.CHUNK_SIZE, limit - total) if limit is not None else self.CHUNK_SIZE
            page = self.client.tool.instances.paginate(instance_filter, limit=page_limit, cursor=cursor)
            total += len(page.items)
            if page:
                yield Page(worker_id="main", items=page.items, next_cursor=page.next_cursor)
            if page.next_cursor is None or (limit is not None and total >= limit) or not page.items:
                break
            cursor = page.next_cursor

    def download_ids(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[Sequence[InstanceId]]:
        # Todo: Switch to use pydantic classes once purge has been updated.
        if isinstance(selector, InstanceFileSelector) and selector.validate_instance is False:
            instances_to_yield = selector.instance_ids
            if limit is not None:
                instances_to_yield = instances_to_yield[:limit]
            yield from chunker_sequence(instances_to_yield, self.CHUNK_SIZE)
        else:
            yield from (
                [
                    NodeId(space=instance.space, external_id=instance.external_id)
                    if instance.instance_type == "node"
                    else EdgeId(space=instance.space, external_id=instance.external_id)
                    for instance in chunk.items
                ]
                for chunk in self.stream_data(selector, limit)
            )

    def count(self, selector: InstanceSelector) -> int | None:
        if isinstance(selector, InstanceViewSelector) or (
            isinstance(selector, InstanceSpaceSelector) and selector.view
        ):
            view_id = cast(SelectedView, selector.view)
            result = self.client.data_modeling.instances.aggregate(
                view=ViewId(space=view_id.space, external_id=view_id.external_id, version=view_id.version),
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
            return len(selector.items)
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Sequence[InstanceResponse], selector: InstanceSelector | None = None
    ) -> list[dict[str, JsonVal]]:
        return [instance.as_request_resource().dump() for instance in data_chunk]

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> InstanceRequest:
        item_to_load = item_json.copy()
        if self._remove_existing_version and "existingVersion" in item_to_load:
            del item_to_load["existingVersion"]
        instance = InstanceRequestAdapter.validate_python(item_to_load)
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
        view_crud = ViewCRUD(self.client, None, None, topological_sort_implements=True)
        views = self.client.tool.views.retrieve([selector.view.as_id()], include_inherited_properties=False)
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
