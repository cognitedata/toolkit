from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Any, ClassVar, Literal, cast

from cognite.client import data_modeling as sdk_dm
from cognite.client.data_classes.aggregations import Count

from cognite_toolkit._cdf_tk import constants
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import (
    ContainerId,
    InstanceDefinitionId,
    SpaceId,
    ViewId,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes import data_modeling as dm
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    NodeOrEdgeRequest,
    NodeOrEdgeResponse,
    QueryEdgeExpression,
    QueryEdgeTableExpression,
    QueryNodeExpression,
    QueryNodeTableExpression,
    QueryRequest,
    QuerySelect,
    QuerySelectSource,
    QuerySortSpec,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._instance import NodeOrEdgeRequestAdapter
from cognite_toolkit._cdf_tk.constants import SUBSELECTION_LIMIT_QUERY_ENDPOINT
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD, SpaceCRUD, ViewIO
from cognite_toolkit._cdf_tk.utils import sanitize_filename
from cognite_toolkit._cdf_tk.utils.collection import chunker_sequence
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.useful_types import DataType, JsonVal

from . import StorageIOConfig
from ._base import Bookmark, ConfigurableDataIO, DataItem, Page, TableDataIO, TableUploadableDataIO
from .logger import LogEntryV2, Severity
from .progress import CursorBookmark, NoBookmark
from .selectors import InstanceFileSelector, InstanceSelector, InstanceSpaceSelector, InstanceViewSelector, SelectedView
from .selectors._instances import InstanceQuerySelector


class InstanceIO(
    ConfigurableDataIO[InstanceSelector, NodeOrEdgeResponse],
    TableDataIO[InstanceSelector, NodeOrEdgeResponse],
    TableUploadableDataIO[InstanceSelector, NodeOrEdgeResponse, NodeOrEdgeRequest],
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
    CHUNK_SIZE = 1000
    UPLOAD_ENDPOINT = "/models/instances"
    UPLOAD_EXTRA_ARGS: ClassVar[Mapping[str, JsonVal] | None] = MappingProxyType(
        {
            "autoCreateDirectRelations": True,
            "autoCreateStartNodes": True,
            "autoCreateEndNodes": True,
            "skipOnVersionConflict": True,
        }
    )
    BASE_SELECTOR = InstanceSelector

    def __init__(self, client: ToolkitClient, remove_existing_version: bool = True) -> None:
        super().__init__(client)
        self._remove_existing_version = remove_existing_version
        # Cache for view to read-only properties mapping
        self._view_readonly_properties_cache: dict[ViewId, set[str]] = {}
        self._view_crud = ViewIO.create_loader(self.client)

    @staticmethod
    def _build_list_filter(selector: InstanceViewSelector | InstanceSpaceSelector) -> InstanceFilter:
        """Build an InstanceFilter from a selector.

        Args:
            selector: The selector to build the filter from.

        Returns:
            An InstanceFilter for the toolkit instances API.
        """
        source: ViewId | None = None
        space: list[str] | None = None

        if isinstance(selector, InstanceViewSelector):
            source = ViewId(
                space=selector.view.space,
                external_id=selector.view.external_id,
                version=selector.view.version or "",
            )
            if selector.instance_spaces:
                space = list(selector.instance_spaces)
        elif isinstance(selector, InstanceSpaceSelector):
            space = [selector.instance_space]
            if selector.view and selector.view.version:
                source = ViewId(
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
            {"hasData": [selector.view.as_id().dump(include_type=True)]},
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

    def _filter_readonly_properties(self, instance: NodeOrEdgeRequest) -> None:
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
            if isinstance(source.source, ViewId):
                if source.source not in self._view_readonly_properties_cache:
                    self._view_readonly_properties_cache[source.source] = self._view_crud.get_readonly_properties(
                        source.source
                    )
                readonly_properties = self._view_readonly_properties_cache[source.source]
            elif isinstance(source.source, ContainerId):
                if source.source.as_tuple() in constants.READONLY_CONTAINER_PROPERTIES:
                    readonly_properties = constants.READONLY_CONTAINER_PROPERTIES[source.source.as_tuple()]
            if not readonly_properties:
                continue
            source.properties = {k: v for k, v in source.properties.items() if k not in readonly_properties}

    def get_schema(self, selector: InstanceSelector) -> list[SchemaColumn]:
        instance_type, view_id = self._get_instance_type_and_view_id(selector)
        instance_columns: list[SchemaColumn] = [
            SchemaColumn(name="space", type="string"),
            SchemaColumn(name="externalId", type="string"),
            SchemaColumn(name="type.space", type="string"),
            SchemaColumn(name="type.externalId", type="string"),
            SchemaColumn(name="existingVersion", type="integer"),
        ]
        if instance_type == "edge":
            instance_columns.extend(
                [
                    SchemaColumn(name="startNode.space", type="string"),
                    SchemaColumn(name="startNode.externalId", type="string"),
                    SchemaColumn(name="endNode.space", type="string"),
                    SchemaColumn(name="endNode.externalId", type="string"),
                ]
            )
        property_columns: list[SchemaColumn] = []
        if view_id is not None:
            views = self.client.tool.views.retrieve([view_id.as_id()], include_inherited_properties=True)
            if not views:
                raise ToolkitValueError(f"View {view_id.as_id()} not found.")
            property_columns = self._get_schema_from_view(views[0])

        return instance_columns + property_columns

    def _get_instance_type_and_view_id(self, selector: InstanceSelector) -> tuple[str, SelectedView | None]:
        if isinstance(selector, InstanceViewSelector):
            return selector.instance_type, selector.view
        elif isinstance(selector, InstanceSpaceSelector):
            return selector.instance_type, selector.view
        else:
            raise NotImplementedError(f"{type(selector).__name__} does not support downloading to table-format.")

    def _get_schema_from_view(self, view: dm.ViewResponse) -> list[SchemaColumn]:
        columns: list[SchemaColumn] = []
        for prop_id, prop in view.properties.items():
            if not isinstance(prop, dm.ViewCorePropertyResponse):
                # We do not include anny edges in the table
                continue

            data_type = self._get_property_type(prop.type)
            is_array = prop.type.list or False if isinstance(prop.type, dm.ListablePropertyTypeDefinition) else False
            if data_type == "json":
                is_array = False
            columns.append(SchemaColumn(name=prop_id, type=data_type, is_array=is_array))
        return columns

    def _get_property_type(self, prop_type: dm.DataType) -> DataType:
        match prop_type:
            case (
                dm.TextProperty()
                | dm.TimeseriesCDFExternalIdReference()
                | dm.FileCDFExternalIdReference()
                | dm.SequenceCDFExternalIdReference()
            ):
                return "string"
            case dm.BooleanProperty():
                return "boolean"
            case dm.Float32Property() | dm.Float64Property():
                return "float"
            case dm.Int32Property() | dm.Int64Property():
                return "integer"
            case dm.TimestampProperty():
                return "timestamp"
            case dm.DateProperty():
                return "date"
            case dm.JSONProperty():
                return "json"
            case dm.DirectNodeRelation():
                return "json"
            case dm.EnumProperty():
                return "string"
            case _:
                return "string"

    def stream_data(
        self,
        selector: InstanceSelector,
        limit: int | None = None,
        bookmark: Bookmark | None = None,
    ) -> Iterable[Page[NodeOrEdgeResponse]]:
        init_cursor = bookmark.cursor if isinstance(bookmark, CursorBookmark) else None
        if isinstance(selector, InstanceViewSelector) and selector.edge_types and selector.instance_type == "node":
            pages = self._instances_with_container_and_edge_properties(selector, limit, init_cursor)
        elif isinstance(selector, InstanceViewSelector | InstanceSpaceSelector):
            pages = self._instances_with_container_properties(selector, limit, init_cursor)
        elif isinstance(selector, InstanceFileSelector):
            for chunk in chunker_sequence(selector.ids, self.CHUNK_SIZE):
                ids = [f"{item.space}:{item.external_id}" for item in chunk]
                self.logger.register(ids)
                items = [
                    DataItem(tracking_id=f"{item.space}:{item.external_id}", item=item)
                    for item in self.client.tool.instances.retrieve(chunk)
                ]
                if missing_ids := (set(ids) - {item.tracking_id for item in items}):
                    for item_id in missing_ids:
                        self.logger.log(
                            LogEntryV2(
                                id=item_id,
                                label="Missing in CDF",
                                severity=Severity.failure,
                                message=f"The {item_id} was not found in CDF",
                            )
                        )
                yield Page(worker_id="main", items=items, bookmark=NoBookmark())
            return
        elif isinstance(selector, InstanceQuerySelector):
            pages = self._instance_by_query(selector.create_query(), limit, init_cursor)
        else:
            raise NotImplementedError()
        yield from (self.emit_registered_page(page) for page in pages)

    def _instances_with_container_and_edge_properties(
        self, selector: InstanceViewSelector, limit: int | None, init_cursor: str | None = None
    ) -> Iterable[Page]:
        instance_filter = self._build_query_filter(selector, "node")
        view_id = selector.view.as_id()
        if not isinstance(view_id, ViewId):
            raise ToolkitValueError("ViewId is required for InstanceViewSelector")
        root = "nodes"
        with_: dict[str, QueryNodeExpression | QueryEdgeExpression] = {
            root: QueryNodeExpression(
                limit=min(self.CHUNK_SIZE, limit) if limit is not None else self.CHUNK_SIZE,
                nodes=QueryNodeTableExpression(filter=instance_filter),
                # Sort to ensure performance. f you do not sort, you get the internal index,
                # which includes all deleted instances as well.
                sort=[QuerySortSpec(property=["node", "space"]), QuerySortSpec(property=["node", "externalId"])],
            )
        }
        select: dict[str, QuerySelect] = {
            root: QuerySelect(sources=[QuerySelectSource(source=view_id, properties=["*"])])
        }
        edge_ids: list[str] = []
        for no, edge_type in enumerate(selector.edge_types or [], start=1):
            query_id = f"edge_{no}"
            with_[query_id] = QueryEdgeExpression(
                limit=SUBSELECTION_LIMIT_QUERY_ENDPOINT,
                edges=QueryEdgeTableExpression(
                    from_=root,
                    chain_to="source" if edge_type.direction == "outwards" else "destination",
                    direction=edge_type.direction,
                    filter={
                        "equals": {
                            "property": ["edge", "type"],
                            "value": edge_type.type.dump(include_instance_type=False),
                        }
                    },
                ),
            )
            edge_ids.append(query_id)
            select[query_id] = QuerySelect()

        query = QueryRequest(with_=with_, select=select, root="nodes")
        yield from self._instance_by_query(query, limit, init_cursor, endpoint=selector.endpoint)

    def _instance_by_query(
        self,
        query: QueryRequest,
        limit: int | None,
        init_cursor: str | None = None,
        endpoint: Literal["query", "sync"] = "query",
    ) -> Iterable[Page]:
        if init_cursor is not None:
            query.cursors = {query.root: init_cursor}

        for batch in self.client.tool.instances.query_iterate(
            query,
            type_results=True,
            exhaust_sub_selections=True,
            limit=limit,
            endpoint=endpoint,
        ):
            wrapped_items = [
                DataItem(tracking_id=f"{item.space}:{item.external_id}", item=item)
                for items in batch.items.values()
                for item in items
            ]
            next_cursor = batch.root_cursor
            yield Page(
                worker_id="main",
                items=wrapped_items,
                bookmark=CursorBookmark(cursor=next_cursor, source="sync" if endpoint == "sync" else "regular")
                if next_cursor
                else NoBookmark(),
            )

    def _instances_with_container_properties(
        self,
        selector: InstanceViewSelector | InstanceSpaceSelector,
        limit: int | None,
        init_cursor: str | None = None,
    ) -> Iterable[Page]:
        instance_filter = self._build_list_filter(selector)
        total = 0
        cursor: str | None = init_cursor
        while cursor is not None or total == 0:
            page_limit = min(self.CHUNK_SIZE, limit - total) if limit is not None else self.CHUNK_SIZE
            page = self.client.tool.instances.paginate(
                instance_filter, limit=page_limit, cursor=cursor, endpoint=selector.endpoint
            )
            total += len(page.items)
            if page:
                wrapped_items = [
                    DataItem(tracking_id=f"{item.space}:{item.external_id}", item=item) for item in page.items
                ]
                yield Page(
                    worker_id="main",
                    items=wrapped_items,
                    bookmark=CursorBookmark(
                        cursor=page.next_cursor, source="sync" if selector.endpoint == "sync" else "regular"
                    )
                    if page.next_cursor
                    else NoBookmark(),
                )
            if page.next_cursor is None or (limit is not None and total >= limit) or not page.items:
                break
            cursor = page.next_cursor

    def download_ids(
        self, selector: InstanceSelector, limit: int | None = None
    ) -> Iterable[Page[InstanceDefinitionId]]:
        if isinstance(selector, InstanceFileSelector) and selector.validate_instance is False:
            instances_to_yield = selector.instance_ids
            if limit is not None:
                instances_to_yield = instances_to_yield[:limit]
            yield from (
                Page(
                    worker_id="main",
                    items=[DataItem(tracking_id=f"{item.space}:{item.external_id}", item=item) for item in chunk],
                )
                for chunk in chunker_sequence(instances_to_yield, self.CHUNK_SIZE)
            )
        else:
            for page in self.stream_data(selector, limit):
                ids = [DataItem(tracking_id=item.tracking_id, item=item.item.as_id()) for item in page.items]
                yield page.create_from(items=ids)

    def count(self, selector: InstanceSelector) -> int | None:
        if isinstance(selector, InstanceViewSelector) or (
            isinstance(selector, InstanceSpaceSelector) and selector.view
        ):
            view_id = cast(SelectedView, selector.view)
            result = self.client.data_modeling.instances.aggregate(
                view=sdk_dm.ViewId(space=view_id.space, external_id=view_id.external_id, version=view_id.version),
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
        elif isinstance(selector, InstanceQuerySelector):
            return None
        raise NotImplementedError()

    def data_to_json_chunk(
        self, data_chunk: Page[NodeOrEdgeResponse], selector: InstanceSelector | None = None
    ) -> Page[dict[str, JsonVal]]:
        result = [
            DataItem(tracking_id=item.tracking_id, item=item.item.as_request_resource().dump())
            for item in data_chunk.items
        ]
        return data_chunk.create_from(result)

    def json_to_resource(self, item_json: dict[str, JsonVal]) -> NodeOrEdgeRequest:
        item_to_load = item_json.copy()
        if self._remove_existing_version and "existingVersion" in item_to_load:
            del item_to_load["existingVersion"]
        instance = NodeOrEdgeRequestAdapter.validate_python(item_to_load)
        self._filter_readonly_properties(instance)
        return instance

    def json_to_row(
        self, item_json: dict[str, JsonVal], selector: InstanceSelector | None = None
    ) -> dict[str, JsonVal]:
        row: dict[str, JsonVal] = {}
        for key, value in item_json.items():
            if key == "sources" and isinstance(value, list):
                for source in value:
                    if not isinstance(source, dict):
                        continue
                    row.update(source.get("properties", {}))  # type: ignore[arg-type]
            elif key == "instanceType":
                # This is stored in the manifest
                continue
            elif isinstance(value, dict):
                for subkey, subvalue in value.items():
                    row[f"{key}.{subkey}"] = subvalue
            else:
                row[key] = value
        return row

    def row_to_resource(
        self, source_id: str, row: dict[str, JsonVal], selector: InstanceSelector | None = None
    ) -> NodeOrEdgeRequest:
        """Convert a row-based dictionary back to an NodeOrEdgeRequest.

        The row format is the inverse of json_to_row:
        - Instance-level fields: `space`, `externalId`, `existingVersion`
        - Nested fields flattened with dots: `type.space`, `type.externalId`, `startNode.space`, etc.
        - Source properties are at the root level (any key not matching the above)

        Args:
            source_id: The source identifier (e.g., row number in a CSV file).
            row: A dictionary representing the instance data in row format.
            selector: The selector used to identify the view for source properties.

        Returns:
            An NodeOrEdgeRequest representing the data.
        """
        # Known instance-level fields and nested field prefixes
        instance_scalar_fields = {"space", "externalId", "existingVersion"}
        nested_field_prefixes = {"type", "startNode", "endNode"}

        instance_fields: dict[str, JsonVal] = {}
        nested_fields: dict[str, dict[str, JsonVal]] = {}
        source_properties: dict[str, JsonVal] = {}

        for key, value in row.items():
            if key in instance_scalar_fields:
                instance_fields[key] = value
            elif "." in key:
                prefix, subkey = key.split(".", 1)
                if prefix in nested_field_prefixes:
                    if prefix not in nested_fields:
                        nested_fields[prefix] = {}
                    nested_fields[prefix][subkey] = value
                else:
                    # Dot in key but not a known prefix - treat as source property
                    source_properties[key] = value
            else:
                # Not an instance field - treat as source property
                source_properties[key] = value

        # Determine instance type: if we have startNode/endNode, it's an edge
        instance_type: Literal["node", "edge"] = "edge" if "startNode" in nested_fields else "node"

        # Build the JSON structure expected by json_to_resource
        item_json: dict[str, JsonVal] = {
            "instanceType": instance_type,
            **instance_fields,
            **nested_fields,
        }

        # Add sources if we have properties and a selector with a view
        if source_properties and isinstance(selector, InstanceViewSelector | InstanceSpaceSelector) and selector.view:
            view_id = selector.view.as_id()
            item_json["sources"] = [
                {
                    "source": view_id.dump(include_type=True),
                    "properties": source_properties,
                }
            ]
        elif source_properties:
            raise NotImplementedError(f"{type(selector).__name__} does not support upload from table format.")

        return self.json_to_resource(item_json)

    def configurations(self, selector: InstanceSelector) -> Iterable[StorageIOConfig]:
        if not isinstance(selector, InstanceViewSelector | InstanceSpaceSelector):
            return
        spaces = list(set((selector.get_instance_spaces() or []) + (selector.get_schema_spaces() or [])))
        if not spaces:
            return
        space_crud = SpaceCRUD.create_loader(self.client)
        retrieved_spaces = space_crud.retrieve([SpaceId(space=space) for space in spaces])
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
        view_crud = ViewIO(self.client, None, None, topological_sort_implements=True)
        views = self.client.tool.views.retrieve([selector.view.as_id()], include_inherited_properties=False)
        views = [view for view in views if not view.is_global]
        if not views:
            return
        for view in views:
            filename = f"{view.space}_{view.external_id}"
            if view.version is not None:
                filename += f"_{view.version}"
            yield StorageIOConfig(
                kind=ViewIO.kind,
                folder_name=ViewIO.folder_name,
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
