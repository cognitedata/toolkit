from abc import ABC, abstractmethod
from collections.abc import Hashable, Iterable, Mapping, Sequence, Set
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import cache
from typing import Any, ClassVar, Generic, Literal, cast

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import AssetCentricExternalId, ExternalId, InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationResponse, AssetLinkData, FileLinkData
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    DirectNodeRelation,
    EdgeId,
    EdgeProperty,
    EdgeRequest,
    EdgeResponse,
    FileCDFExternalIdReference,
    InstanceResponse,
    InstanceSource,
    NodeId,
    NodeRequest,
    NodeResponse,
    SingleEdgeProperty,
    TimeseriesCDFExternalIdReference,
    ViewCorePropertyResponse,
    ViewId,
    ViewResponse,
    ViewResponseProperty,
)
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import AssetCentricId
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import ResourceViewMappingRequest
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.utils.collection import flatten_dict_json_path
from cognite_toolkit._cdf_tk.utils.dms import serialize_dms
from cognite_toolkit._cdf_tk.utils.dtype_conversion import (
    asset_centric_convert_to_primary_property,
    convert_to_primary_property,
)
from cognite_toolkit._cdf_tk.utils.text import sanitize_instance_external_id
from cognite_toolkit._cdf_tk.utils.useful_types import T_ID, AssetCentricTypeExtended
from cognite_toolkit._cdf_tk.utils.useful_types2 import AssetCentricResourceExtended

from .data_model import COGNITE_MIGRATION_SPACE_ID, INSTANCE_SOURCE_VIEW_ID
from .issues import ConversionIssue, FailedConversion, InvalidPropertyDataType


class DirectRelationCache:
    """Cache for direct relation references to look up target of direct relations.

    This is used when creating direct relations from asset-centric resources to assets, files, and source systems.
    """

    class TableName:
        ASSET_ID = "assetId"
        SOURCE_NAME = "source"
        FILE_ID = "fileId"
        ASSET_EXTERNAL_ID = "assetExternalId"
        FILE_EXTERNAL_ID = "fileExternalId"

    ASSET_ID_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {
        ("timeseries", "assetId"),
        ("file", "assetIds"),
        ("event", "assetIds"),
        ("sequence", "assetId"),
        ("annotation", "data.assetRef.id"),
        ("asset", "parentId"),
    }
    SOURCE_NAME_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {
        ("asset", "source"),
        ("event", "source"),
        ("file", "source"),
    }
    FILE_ID_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {
        ("annotation", "data.fileRef.id"),
        ("annotation", "annotatedResourceId"),
    }
    ASSET_EXTERNAL_ID_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {("annotation", "data.assetRef.externalId")}
    FILE_EXTERNAL_ID_PROPERTIES: ClassVar[Set[tuple[str, str]]] = {("annotation", "data.fileRef.externalId")}

    def __init__(self, client: ToolkitClient) -> None:
        self._client = client
        self._cache_map: dict[tuple[str, str] | str, dict[str, NodeId] | dict[int, NodeId]] = {}
        # Constructing the cache map to be accessed by both table name and property id
        for table_name, properties in [
            (self.TableName.ASSET_ID, self.ASSET_ID_PROPERTIES),
            (self.TableName.SOURCE_NAME, self.SOURCE_NAME_PROPERTIES),
            (self.TableName.FILE_ID, self.FILE_ID_PROPERTIES),
            (self.TableName.ASSET_EXTERNAL_ID, self.ASSET_EXTERNAL_ID_PROPERTIES),
            (self.TableName.FILE_EXTERNAL_ID, self.FILE_EXTERNAL_ID_PROPERTIES),
        ]:
            cache: dict[str, NodeId] | dict[int, NodeId] = {}
            self._cache_map[table_name] = cache
            for key in properties:
                self._cache_map[key] = cache

    def update(self, resources: Iterable[AssetCentricResourceExtended]) -> None:
        """Update the cache with direct relation references for the given asset-centric resources.

        This is used to bulk update the cache for a chunk of resources before converting them to data model instances.
        """
        asset_ids: set[int] = set()
        source_ids: set[str] = set()
        file_ids: set[int] = set()
        asset_external_ids: set[str] = set()
        file_external_ids: set[str] = set()
        for resource in resources:
            if isinstance(resource, AnnotationResponse):
                if resource.annotated_resource_type == "file" and resource.annotated_resource_id:
                    file_ids.add(resource.annotated_resource_id)
                self._extract_annotation_refs(resource.data, asset_ids, asset_external_ids, file_ids, file_external_ids)
            elif isinstance(resource, AssetResponse):
                if resource.source:
                    source_ids.add(resource.source)
                if resource.parent_id is not None:
                    asset_ids.add(resource.parent_id)
            elif isinstance(resource, FileMetadataResponse):
                if resource.source:
                    source_ids.add(resource.source)
                if resource.asset_ids:
                    asset_ids.update(resource.asset_ids)
            elif isinstance(resource, EventResponse):
                if resource.source:
                    source_ids.add(resource.source)
                if resource.asset_ids:
                    asset_ids.update(resource.asset_ids)
            elif isinstance(resource, TimeSeriesResponse):
                if resource.asset_id is not None:
                    asset_ids.add(resource.asset_id)
        if asset_ids:
            self._update_cache(self._client.migration.lookup.assets(id=list(asset_ids)), self.TableName.ASSET_ID)
        if source_ids:
            # SourceSystems are not cached in the client, so we have to handle the caching ourselves.
            cache = cast(dict[str, NodeId], self._cache_map[self.TableName.SOURCE_NAME])
            missing: dict[str, str] = {}
            for source_id in source_ids:
                if source_id.casefold() not in cache:
                    missing[source_id.casefold()] = source_id
                elif source_id not in cache:
                    missing[source_id] = source_id
            if missing:
                source_systems = self._client.migration.created_source_system.retrieve(list(missing))
                for source_system in source_systems:
                    source_reference = source_system.as_direct_relation_reference()
                    cache[source_system.source] = source_reference
                    if original_str := missing.get(source_system.source):
                        cache[original_str] = source_reference
        if file_ids:
            self._update_cache(self._client.migration.lookup.files(id=list(file_ids)), self.TableName.FILE_ID)
        if asset_external_ids:
            self._update_cache(
                self._client.migration.lookup.assets(external_id=list(asset_external_ids)),
                self.TableName.ASSET_EXTERNAL_ID,
            )
        if file_external_ids:
            self._update_cache(
                self._client.migration.lookup.files(external_id=list(file_external_ids)),
                self.TableName.FILE_EXTERNAL_ID,
            )

    @staticmethod
    def _extract_annotation_refs(
        data: AssetLinkData | FileLinkData | dict[str, Any],
        asset_ids: set[int],
        asset_external_ids: set[str],
        file_ids: set[int],
        file_external_ids: set[str],
    ) -> None:
        if isinstance(data, AssetLinkData):
            if isinstance(data.asset_ref, InternalId):
                asset_ids.add(data.asset_ref.id)
            elif isinstance(data.asset_ref, ExternalId):
                asset_external_ids.add(data.asset_ref.external_id)
        elif isinstance(data, FileLinkData):
            if isinstance(data.file_ref, InternalId):
                file_ids.add(data.file_ref.id)
            elif isinstance(data.file_ref, ExternalId):
                file_external_ids.add(data.file_ref.external_id)

    def _update_cache(self, instance_id_by_id: dict[int, NodeId] | dict[str, NodeId], table_name: str) -> None:
        cache = self._cache_map[table_name]
        for identifier, instance_id in instance_id_by_id.items():
            cache[identifier] = NodeId(space=instance_id.space, external_id=instance_id.external_id)  # type: ignore[index]

    def get_cache(self, resource_type: AssetCentricTypeExtended, property_id: str) -> Mapping[str | int, NodeId] | None:
        """Get the cache for the given resource type and property ID."""
        return self._cache_map.get((resource_type, property_id))  # type: ignore[return-value]


def asset_centric_to_dm(
    resource: AssetCentricResourceExtended,
    instance_id: NodeId | EdgeId,
    view_source: ResourceViewMappingRequest,
    view_properties: dict[str, ViewResponseProperty],
    direct_relation_cache: DirectRelationCache,
    preferred_consumer_view: ViewId | None = None,
) -> tuple[NodeRequest | EdgeRequest | None, ConversionIssue]:
    """Convert an asset-centric resource to a data model instance.

    Args:
        resource (CogniteResource): The asset-centric resource to convert.
        instance_id (NodeId | EdgeApply): The ID of the instance to create or update.
        view_source (ResourceViewMappingRequest): The view source defining how to map the resource to the data model.
        view_properties (dict[str, ViewProperty]): The defined properties referenced in the view source mapping.
        direct_relation_cache (DirectRelationCache): Cache for direct relation references.
        preferred_consumer_view (ViewId | None): The preferred consumer view for the instance.

    Returns:
        tuple[NodeApply | EdgeApply, ConversionIssue]: A tuple containing the converted NodeApply and any ConversionIssue encountered.
    """
    resource_type = _lookup_resource_type(resource)
    dumped = resource.dump()
    try:
        id_ = dumped.pop("id")
    except KeyError as e:
        raise ValueError("Resource must have an 'id' field.") from e
    if not isinstance(id_, int):
        raise TypeError(f"Resource 'id' field must be an int, got {type(id_)}.")
    data_set_id = dumped.pop("dataSetId", None)
    external_id = dumped.pop("externalId", None)

    issue = ConversionIssue(
        id=str(AssetCentricId(resource_type, id_=id_)),
        asset_centric_id=AssetCentricId(resource_type, id_=id_),
        instance_id=NodeId(space=instance_id.space, external_id=instance_id.external_id),
    )

    properties = create_properties(
        dumped,
        view_properties,
        view_source.property_mapping,
        resource_type,
        issue=issue,
        direct_relation_cache=direct_relation_cache,
    )
    sources: list[InstanceSource] = []
    if properties:
        sources.append(
            InstanceSource(
                source=ViewId(
                    space=view_source.view_id.space,
                    external_id=view_source.view_id.external_id,
                    version=view_source.view_id.version,
                ),
                properties=properties,
            )
        )

    if resource_type != "annotation":
        instance_source_properties = {
            "resourceType": resource_type,
            "id": id_,
            "dataSetId": data_set_id,
            "classicExternalId": external_id,
            "resourceViewMapping": {"space": COGNITE_MIGRATION_SPACE_ID, "externalId": view_source.external_id},
        }
        if preferred_consumer_view:
            instance_source_properties["preferredConsumerViewId"] = {
                **preferred_consumer_view.dump(),
                "type": "view",
            }
        sources.append(
            InstanceSource(
                source=ViewId(
                    space=INSTANCE_SOURCE_VIEW_ID.space,
                    external_id=INSTANCE_SOURCE_VIEW_ID.external_id,
                    version=view_source.view_id.version,
                ),
                properties=instance_source_properties,
            )
        )

    instance: NodeRequest | EdgeRequest
    if isinstance(instance_id, EdgeId):
        edge_properties = create_edge_properties(
            dumped, view_source.property_mapping, resource_type, issue, direct_relation_cache, instance_id.space
        )
        if any(key not in edge_properties for key in ("start_node", "end_node", "type")):
            # Failed conversion of edge properties
            return None, issue
        instance = EdgeRequest(
            space=instance_id.space,
            external_id=instance_id.external_id,
            sources=sources,
            **edge_properties,  # type: ignore[arg-type]
        )
    elif isinstance(instance_id, NodeId):
        instance = NodeRequest(space=instance_id.space, external_id=instance_id.external_id, sources=sources)
    else:
        raise RuntimeError(f"Unexpected instance_id type {type(instance_id)}")

    return instance, issue


def _lookup_resource_type(resource_type: AssetCentricResourceExtended) -> AssetCentricTypeExtended:
    if isinstance(resource_type, AssetResponse):
        return "asset"
    elif isinstance(resource_type, FileMetadataResponse):
        return "file"
    elif isinstance(resource_type, EventResponse):
        return "event"
    elif isinstance(resource_type, TimeSeriesResponse):
        return "timeseries"
    elif isinstance(resource_type, AnnotationResponse):
        if resource_type.annotated_resource_type == "file" and resource_type.annotation_type in (
            "diagrams.AssetLink",
            "diagrams.FileLink",
        ):
            return "annotation"
    raise ValueError(f"Unsupported resource type: {resource_type}")


def create_properties(
    dumped: dict[str, Any],
    view_properties: dict[str, ViewResponseProperty],
    property_mapping: dict[str, str],
    resource_type: AssetCentricTypeExtended,
    issue: ConversionIssue,
    direct_relation_cache: DirectRelationCache,
) -> dict[str, JsonValue]:
    """
    Create properties for a data model instance from an asset-centric resource.

    Args:
        dumped: Dict representation of the asset-centric resource.
        view_properties: Defined properties referenced in the view source mapping.
        property_mapping:  Mapping from asset-centric property IDs to data model property IDs.
        resource_type: The type of the asset-centric resource (e.g., "asset", "timeseries").
        issue: ConversionIssue object to log any issues encountered during conversion.
        direct_relation_cache: Cache for direct relation references to look up target of direct relations.

    Returns:
        Dict of property IDs to PropertyValueWrite objects.

    """
    flatten_dump = flatten_dict_json_path(dumped, keep_structured=set(property_mapping.keys()))
    properties: dict[str, JsonValue] = {}
    ignored_asset_centric_properties: set[str] = set()
    for prop_json_path, prop_id in property_mapping.items():
        if prop_json_path not in flatten_dump:
            continue
        if prop_id not in view_properties:
            continue
        if prop_id in properties:
            ignored_asset_centric_properties.add(prop_json_path)
            continue
        dm_prop = view_properties[prop_id]
        if not isinstance(dm_prop, ViewCorePropertyResponse):
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=prop_id, expected_type=ViewCorePropertyResponse.__name__)
            )
            continue
        try:
            value = asset_centric_convert_to_primary_property(
                flatten_dump[prop_json_path],
                dm_prop.type,
                dm_prop.nullable or False,
                destination_container_property=(dm_prop.container, dm_prop.container_property_identifier),
                source_property=(resource_type, prop_json_path),
                direct_relation_lookup=direct_relation_cache.get_cache(resource_type, prop_json_path),
            )
        except (ValueError, TypeError, NotImplementedError) as e:
            issue.failed_conversions.append(
                FailedConversion(property_id=prop_json_path, value=flatten_dump[prop_json_path], error=str(e))
            )
            continue
        if isinstance(value, date):
            # Convert date to ISO format string, as the data model expects dates as strings in ISO format.
            properties[prop_id] = value.isoformat()
        elif isinstance(value, datetime):
            # Convert datetime to ISO format string, as the data model expects datetimes as strings in ISO format.
            properties[prop_id] = value.isoformat(timespec="milliseconds")
        else:
            properties[prop_id] = value

    issue.ignored_asset_centric_properties = sorted(
        (set(flatten_dump.keys()) - set(property_mapping.keys())) | ignored_asset_centric_properties
    )
    issue.missing_asset_centric_properties = sorted(set(property_mapping.keys()) - set(flatten_dump.keys()))
    # Node and edge properties are handled separately
    issue.missing_instance_properties = sorted(
        {
            prop_id
            for prop_id in property_mapping.values()
            if not (prop_id.startswith("edge.") or prop_id.startswith("node."))
        }
        - set(view_properties.keys())
    )
    return properties


def create_edge_properties(
    dumped: dict[str, Any],
    property_mapping: dict[str, str],
    resource_type: AssetCentricTypeExtended,
    issue: ConversionIssue,
    direct_relation_cache: DirectRelationCache,
    default_instance_space: str,
) -> dict[str, NodeId]:
    flatten_dump = flatten_dict_json_path(dumped)
    edge_properties: dict[str, NodeId] = {}
    for prop_json_path, prop_id in property_mapping.items():
        if not prop_id.startswith("edge."):
            continue
        if prop_json_path not in flatten_dump:
            continue
        edge_prop_id = prop_id.removeprefix("edge.")
        if edge_prop_id in ("startNode", "endNode", "type"):
            value: NodeId | Any
            # DirectRelation lookup.
            try:
                value = convert_to_primary_property(
                    flatten_dump[prop_json_path],
                    DirectNodeRelation(),
                    False,
                    direct_relation_lookup=direct_relation_cache.get_cache(resource_type, prop_json_path),
                )
            except (ValueError, TypeError, NotImplementedError) as e:
                issue.failed_conversions.append(
                    FailedConversion(property_id=prop_json_path, value=flatten_dump[prop_json_path], error=str(e))
                )
                continue
        elif edge_prop_id.endswith(".externalId"):
            # Just an external ID string.
            edge_prop_id = edge_prop_id.removesuffix(".externalId")
            value = NodeId(space=default_instance_space, external_id=str(flatten_dump[prop_json_path]))
        else:
            issue.invalid_instance_property_types.append(
                InvalidPropertyDataType(property_id=prop_id, expected_type="EdgeProperty")
            )
            continue
        edge_properties[edge_prop_id.replace("Node", "_node")] = value  # type: ignore[assignment]

    return edge_properties


class TimeSeriesFilesReferenceCache:
    """Cache for looking up timeseries/files reference in classic to find the matching instance ID"""

    def __init__(self, client: ToolkitClient) -> None:
        self._client = client
        self._cache: dict[Literal["timeseries", "file"], dict[str, NodeId]] = {
            "timeseries": {},
            "file": {},
        }

    def update(self, resource_type: Literal["timeseries", "file"], resource_ids: list[str]) -> list[str]:
        resources: list[TimeSeriesResponse] | list[FileMetadataResponse]
        ids = ExternalId.from_external_ids(resource_ids)
        if resource_type == "timeseries":
            resources = self._client.tool.timeseries.retrieve(ids, ignore_unknown_ids=True)
        elif resource_type == "file":
            resources = self._client.tool.filemetadata.retrieve(ids, ignore_unknown_ids=True)
        else:
            raise ValueError(f"Unsupported resource type: {resource_type}")
        missing_instance_ids: list[str] = []
        for resource in resources:
            # We do the lookup on ExternalID, so we know that it will always be set.
            external_id = cast(str, resource.external_id)
            if resource.instance_id is None:
                missing_instance_ids.append(external_id)
            else:
                self._cache[resource_type][external_id] = resource.instance_id
        return missing_instance_ids

    def get_cache(self, resource_type: Literal["timeseries", "file"]) -> dict[str, NodeId]:
        return self._cache.get(resource_type, {})


@dataclass
class EdgeOtherSide:
    edge_id: EdgeId
    other_side: NodeId


class InstanceToInstanceSpecialMapping(ABC, Generic[T_ID]):
    """
    This class is used for special mapping cases in the instance to instance conversion.

    The main motivation for it is the InField Data Mapping.
    """

    VIEW_PROPERTIES: ClassVar[frozenset[tuple[ViewId, str]]] = frozenset()

    @abstractmethod
    def __getitem__(self, item: T_ID) -> NodeId:
        raise NotImplementedError

    @abstractmethod
    def update(self, items: Iterable[T_ID]) -> None:
        raise NotImplementedError()


class InFieldDataMapping(InstanceToInstanceSpecialMapping[NodeId]):
    """Special cases in the InField data migration

    These are reference to classical assets which are mirrored into FDM. We look up these in the CogniteMigration
    model.

    """

    VIEW_PROPERTIES = frozenset(
        {
            (ViewId(space="cdf_apm", external_id="Activity", version="v2"), "asset"),
            (ViewId(space="cdf_apm", external_id="Checklist", version="v7"), "rootLocation"),
            (ViewId(space="cdf_apm", external_id="ChecklistItem", version="v7"), "asset"),
            (ViewId(space="cdf_apm", external_id="Observation", version="v5"), "asset"),
            (ViewId(space="cdf_apm", external_id="Observation", version="v5"), "rootLocation"),
            (ViewId(space="cdf_apm", external_id="Template", version="v8"), "rootLocation"),
            (ViewId(space="cdf_apm", external_id="TemplateItem", version="v7"), "asset"),
        }
    )

    def __init__(self, client: ToolkitClient) -> None:
        self.client = client
        # We use None to indicate that we have looked up the reference, but it was not found,
        # this allows us to distinguish between "not looked up yet" and "looked up but not found",
        # which is important to avoid repeated lookups for missing references.
        self._node_id_by_external_id: dict[str, NodeId | None] = {}

    def __getitem__(self, item: NodeId) -> NodeId:
        # We ignore the space and assume external ID matches
        # the external ID classic.
        result = self._node_id_by_external_id[item.external_id]
        if result is None:
            raise KeyError(f"No mapping found for {item!r}")
        return result

    def update(self, items: Iterable[NodeId]) -> None:
        external_ids = {item.external_id for item in items}
        missing_external_ids = external_ids - set(self._node_id_by_external_id.keys())
        if missing_external_ids:
            results = self.client.migration.instance_source.retrieve(
                external_ids=AssetCentricExternalId.from_external_ids("asset", missing_external_ids)
            )
            for result in results:
                if result.classic_external_id:
                    self._node_id_by_external_id[result.classic_external_id] = NodeId(
                        space=result.space, external_id=result.external_id
                    )
                else:
                    self._node_id_by_external_id[result.external_id] = None
            failed_lookups = missing_external_ids - set(self._node_id_by_external_id.keys())
            for failed in failed_lookups:
                self._node_id_by_external_id[failed] = None


class ConnectionCreator:
    """Used to create connections (edges and direct relations) between migrated instances.

    It is used both in the convert_container_properties and convert edges functions.

    It keeps track of how all connections needs to be converted from a source to target spaces. As
    well as deal with timeseries and files reference conversion.

    Args:
        client: ToolkitClient to use for lookups when creating connections.
        space_mapping: Mapping from source space IDs to destination space IDs, used to map instance IDs from source to destination space when creating connections.
        special_cases: Optional mapping for any special cases where the mapping from source to destination instance ID cannot be handled by the space mapping or timeseries/files reference cache. The keys are tuples of (source_view_id, source_prop_id) and the values are mappings from source instance IDs (either external ID or NodeId) to destination NodeIds.

    """

    def __init__(
        self,
        client: ToolkitClient,
        space_mapping: Mapping[str, str],
        special_cases: Sequence[InstanceToInstanceSpecialMapping] | None = None,
    ) -> None:
        self._client = client
        self.space_mapping = space_mapping
        self.view_by_id: dict[ViewId, ViewResponse] = {}
        self._special_cases: Sequence[InstanceToInstanceSpecialMapping] = special_cases or []
        self._special_case_caches = self._create_special_case_caches(special_cases or [])
        self._timeseries_reference_cache: dict[str, NodeId] = {}
        self._file_reference_cache: dict[str, NodeId] = {}

    def _create_special_case_caches(
        self, special_cases: Sequence[InstanceToInstanceSpecialMapping]
    ) -> dict[tuple[ViewId, str], InstanceToInstanceSpecialMapping]:
        mapping: dict[tuple[ViewId, str], InstanceToInstanceSpecialMapping] = {}
        for case in special_cases:
            for view_property in case.VIEW_PROPERTIES:
                mapping[view_property] = case
        return mapping

    def update_view_cache(self, views: Iterable[ViewResponse]) -> None:
        for view in views:
            self.view_by_id[view.as_id()] = view

    def update_cache(self, instances: Sequence[InstanceResponse]) -> None:
        self._update_views(instances)
        self._update_property_caches(instances)

    def _update_views(self, instances: Sequence[InstanceResponse]) -> None:
        unique_views = {
            view_id for item in instances for view_id in (item.properties or {}).keys() if isinstance(view_id, ViewId)
        }
        missing_views = unique_views - set(self.view_by_id.keys())
        if missing_views:
            views = self._client.tool.views.retrieve(list(missing_views))
            for view in views:
                self.view_by_id[view.as_id()] = view

    def _update_property_caches(self, instances: Sequence[InstanceResponse]) -> None:
        timeseries_refs: set[str] = set()
        file_refs: set[str] = set()
        special_cases_keys: dict[tuple[ViewId, str], tuple[InstanceToInstanceSpecialMapping, set[Hashable]]] = {}
        for special_caches in self._special_cases:
            # We create the keys for this special case cache here. This is to ensure all
            # view properties will write to the same set.
            keys: set[Hashable] = set()
            for view_property in special_caches.VIEW_PROPERTIES:
                special_cases_keys[view_property] = (special_caches, keys)
        for item in instances:
            for view_id, properties in (item.properties or {}).items():
                if not isinstance(view_id, ViewId):
                    continue
                for prop_id, value in properties.items():
                    if self._is_timeseries_reference(view_id, prop_id):
                        timeseries_refs.update(self._as_str_iterable(value))
                    elif self._is_file_reference(view_id, prop_id):
                        file_refs.update(self._as_str_iterable(value))
                    if (view_id, prop_id) in special_cases_keys:
                        special_cases_keys[(view_id, prop_id)][1].update(self._as_hashable_iterable(value))
        if timeseries_refs:
            missing_timeseries_refs = timeseries_refs - set(self._timeseries_reference_cache.keys())
            if missing_timeseries_refs:
                missing_ts = self._client.tool.timeseries.retrieve(
                    ExternalId.from_external_ids(missing_timeseries_refs), ignore_unknown_ids=True
                )
                for ts in missing_ts:
                    if ts.external_id and ts.instance_id:
                        self._timeseries_reference_cache[ts.external_id] = ts.instance_id
        if file_refs:
            missing_file_refs = file_refs - set(self._file_reference_cache.keys())
            if missing_file_refs:
                missing_refs = self._client.tool.filemetadata.retrieve(
                    ExternalId.from_external_ids(missing_file_refs), ignore_unknown_ids=True
                )
                for file in missing_refs:
                    if file.external_id and file.instance_id:
                        self._file_reference_cache[file.external_id] = file.instance_id
        for special_cache, keys in special_cases_keys.values():
            special_cache.update(keys)

    def _as_str_iterable(self, value: Any) -> Iterable[str]:
        if isinstance(value, str):
            yield value
        elif isinstance(value, Iterable):
            for item in value:
                if isinstance(item, str):
                    yield item

    def _as_hashable_iterable(self, value: Any) -> Iterable[Hashable]:
        if isinstance(value, dict):
            res = self._as_node_id(value)
            if res:
                yield res
        elif isinstance(value, Hashable):
            yield value
        elif isinstance(value, Iterable):
            for item in value:
                if isinstance(item, dict):
                    res = self._as_node_id(item)
                    if res:
                        yield res
                elif isinstance(item, Hashable):
                    yield item

    def _get_view_property(self, source_prop_id: str, source_view_id: ViewId) -> ViewResponseProperty | None:
        if source_view_id not in self.view_by_id:
            return None
        view = self.view_by_id[source_view_id]
        return view.properties.get(source_prop_id)

    def _create_targets(
        self, value: Any, source_prop_id: str, source_view_id: ViewId
    ) -> tuple[list[NodeId], list[str]]:
        if isinstance(value, list):
            targets: list[NodeId] = []
            issues: list[str] = []
            for item in value:
                try:
                    targets.append(self._create_target(item, source_prop_id, source_view_id))
                except KeyError:
                    issues.append(f"Failed to create target for value {item!s}")
            return targets, issues
        else:
            try:
                return [self._create_target(value, source_prop_id, source_view_id)], []
            except KeyError:
                return [], [f"Failed to create target for value {value!s}"]

    def _create_target(self, value: Any, source_prop_id: str, source_view_id: ViewId) -> NodeId:
        if special_case_cache := self._special_case_caches.get((source_view_id, source_prop_id)):
            node_id = self._as_node_id(value)
            return special_case_cache[node_id] if node_id else special_case_cache[value]
        elif self._is_timeseries_reference(source_view_id, source_prop_id) and isinstance(value, str):
            return self._timeseries_reference_cache[value]
        elif self._is_file_reference(source_view_id, source_prop_id) and isinstance(value, str):
            return self._file_reference_cache[value]
        elif self._is_direct_relation(source_view_id, source_prop_id) and (node_id := self._as_node_id(value)):
            return self.map_instance(node_id)
        else:
            raise ValueError(
                f"Cannot create connection. Unsupported {source_prop_id!r} property with value {value!r} in view {source_view_id.dump(include_type=True)!r}"
            )

    def _as_node_id(self, value: Any) -> NodeId | None:
        try:
            return NodeId.model_validate(value)
        except ValueError:
            return None

    def map_instance(self, node_id: NodeId | EdgeId | NodeResponse | EdgeResponse) -> NodeId:
        """Maps a node ID form the source view's space to the corresponding node ID in the destination view's space using the space mapping."""
        return NodeId(space=self.space_mapping[node_id.space], external_id=node_id.external_id)

    def edges(self, view_id: ViewId) -> dict[str, EdgeProperty]:
        """Get the edge properties for a given view ID."""
        if view_id not in self.view_by_id:
            raise ValueError(f"View {view_id.dump(include_type=True)!r} not found in cache.")
        view = self.view_by_id[view_id]
        return {prop_id: prop for prop_id, prop in view.properties.items() if isinstance(prop, EdgeProperty)}

    @cache
    def _is_timeseries_reference(self, source_view_id: ViewId, source_prop_id: str) -> bool:
        prop = self._get_view_property(source_prop_id, source_view_id)
        return isinstance(prop, ViewCorePropertyResponse) and isinstance(prop.type, TimeseriesCDFExternalIdReference)

    @cache
    def _is_file_reference(self, source_view_id: ViewId, source_prop_id: str) -> bool:
        prop = self._get_view_property(source_prop_id, source_view_id)
        return isinstance(prop, ViewCorePropertyResponse) and isinstance(prop.type, FileCDFExternalIdReference)

    @cache
    def _is_direct_relation(self, source_view_id: ViewId, source_prop_id: str) -> bool:
        prop = self._get_view_property(source_prop_id, source_view_id)
        return isinstance(prop, ViewCorePropertyResponse) and isinstance(prop.type, DirectNodeRelation)

    def create_edges(
        self, value: Any, dm_prop: EdgeProperty, source_prop_id: str, source_view_id: ViewId, source_id: NodeId
    ) -> tuple[list[EdgeRequest], list[str]]:
        targets, issues = self._create_targets(value, source_prop_id, source_view_id)
        if isinstance(dm_prop, SingleEdgeProperty) and len(targets) > 1:
            issues.append(
                f"Too many targets for edge property {source_prop_id!r} in view {source_view_id.dump(include_type=True)!r}: expected at most 1, got {len(targets)}. Only the first target will be used."
            )
            targets = targets[:1]
        edges: list[EdgeRequest] = []
        for target in targets:
            start_node, end_node = source_id, target
            if dm_prop.direction == "inwards":
                start_node, end_node = end_node, start_node

            edge = EdgeRequest(
                space=source_id.space,
                external_id=sanitize_instance_external_id(
                    f"{source_id.external_id}_{dm_prop.type.external_id}_{target.external_id}"
                ),
                type=dm_prop.type,
                start_node=start_node,
                end_node=end_node,
            )
            edges.append(edge)
        return edges, issues

    def create_direct_relation(
        self, value: Any, dm_prop: DirectNodeRelation, source_prop_id: str, source_view_id: ViewId
    ) -> tuple[NodeId | list[NodeId], list[str]]:
        targets, target_issues = self._create_targets(value, source_prop_id, source_view_id)
        relations, relation_issues = self._targets_to_direct_relation(targets, dm_prop, source_prop_id, source_view_id)
        return relations, target_issues + relation_issues

    def _targets_to_direct_relation(
        self, targets: list[NodeId], dm_prop: DirectNodeRelation, source_prop_id: str, source_view_id: ViewId
    ) -> tuple[NodeId | list[NodeId], list[str]]:
        errors: list[str] = []
        if dm_prop.list:
            if dm_prop.max_list_size and len(targets) > dm_prop.max_list_size:
                targets = targets[: dm_prop.max_list_size]
                errors.append(
                    f"Too many targets for direct relation property {source_prop_id!r} in view {source_view_id.dump(include_type=True)!r}: expected at most {dm_prop.max_list_size}, got {len(targets)}. Truncated to the first {dm_prop.max_list_size} targets."
                )
            return targets, errors
        elif len(targets) == 1:
            return targets[0], errors
        elif len(targets) == 0:
            raise ValueError(
                f"No targets for direct relation property {source_prop_id!r} in view {source_view_id.dump(include_type=True)!r}: expected exactly 1, got 0"
            )
        else:
            errors.append(
                f"Too many targets for direct relation property {source_prop_id!r} in view {source_view_id.dump(include_type=True)!r}: expected exactly 1, got {len(targets)}. Returning the first target."
            )
            return targets[0], errors

    def create_direct_relation_from_edges(
        self, edges: list[EdgeOtherSide], dm_prop: DirectNodeRelation, source_prop_id: str, source_view_id: ViewId
    ) -> tuple[NodeId | list[NodeId], list[str]]:
        targets = [self.map_instance(edge.other_side) for edge in edges]
        return self._targets_to_direct_relation(targets, dm_prop, source_prop_id, source_view_id)

    def create_edges_from_edges(
        self,
        edges: list[EdgeOtherSide],
        dm_prop: EdgeProperty,
        source_prop_id: str,
        source_view_id: ViewId,
        source_id: NodeId,
    ) -> tuple[list[EdgeRequest], list[str]]:
        issues: list[str] = []
        new_edges: list[EdgeRequest] = []
        for edge in edges:
            try:
                new_edge_id = self.map_instance(edge.edge_id)
            except KeyError as e:
                issues.append(f"Failed to map edge ID {edge.edge_id!s} to destination space: {e!s}")
                continue
            try:
                other_side = self.map_instance(edge.other_side)
            except KeyError as e:
                issues.append(
                    f"Failed to map other side of {source_id!s} node ID {edge.other_side!s} to destination space: {e!s}"
                )
                continue

            start_node, end_node = source_id, other_side
            if dm_prop.direction == "inwards":
                start_node, end_node = end_node, start_node
            new_edges.append(
                EdgeRequest(
                    space=new_edge_id.space,
                    external_id=new_edge_id.external_id,
                    type=dm_prop.type,
                    start_node=start_node,
                    end_node=end_node,
                )
            )
        return new_edges, issues


@dataclass
class ConversionResult:
    container_properties: dict[str, JsonValue]
    errors: list[str] = field(default_factory=list)
    edges: list[EdgeRequest] = field(default_factory=list)


def convert_container_properties(
    source_properties: dict[str, JsonValue],
    mapping: ViewToViewMapping,
    destination_properties: dict[str, ViewResponseProperty],
    connection_creator: ConnectionCreator,
    source_view_id: ViewId,
    source_id: NodeId,
) -> ConversionResult:
    """
    Create properties for a data model instance from another instance's properties.

    This is used for node-to-node conversion when the source and destination views have different defined properties.

    Args:
        source_properties: Dict of source property IDs to values.
        mapping: The ViewToViewMapping defining how to map properties from the source view to the destination view.
        destination_properties: Dict of defined properties in the destination view.
        connection_creator: Helper object to create connections (edges and direct relations) based on property values.
        source_view_id: The ID of the source view, used for error messages.
        source_id: The ID of the source instance used for edge creation and error messages.
    """
    created_properties: dict[str, JsonValue] = {}
    edges: list[EdgeRequest] = []
    errors: list[str] = []
    for source_prop_id, value in source_properties.items():
        dest_prop_id = mapping.get_destination_property(source_prop_id)
        if not dest_prop_id or (
            dest_prop_id not in destination_properties and dest_prop_id not in mapping.property_mapping
        ):
            # We do not warn about the node properties, as they are typically ignored.
            if not source_prop_id.startswith("node."):
                errors.append(f"Source instance property {source_prop_id!r} is not mapped to any destination property.")
            continue
        if dest_prop_id not in destination_properties:
            errors.append(f"Destination instance is missing property {dest_prop_id!r}.")
            continue

        dm_prop = destination_properties[dest_prop_id]
        if isinstance(dm_prop, EdgeProperty):
            try:
                created_edges, issues = connection_creator.create_edges(
                    value,
                    dm_prop,
                    source_prop_id,
                    source_view_id,
                    source_id,
                )
            except ValueError as e:
                errors.append(f"Failed to create edges for property {source_prop_id!r} with value {value!r}: {e!s}")
                continue
            edges.extend(created_edges)
            errors.extend(issues)
        elif isinstance(dm_prop, ViewCorePropertyResponse) and isinstance(dm_prop.type, DirectNodeRelation):
            try:
                created_connection, issues = connection_creator.create_direct_relation(
                    value,
                    dm_prop.type,
                    source_prop_id,
                    source_view_id,
                )
            except ValueError as e:
                errors.append(
                    f"Failed to create direct relation for property {source_prop_id!r} with value {value!r}: {e!s}"
                )
                continue
            errors.extend(issues)
            if isinstance(created_connection, list):
                created_properties[dest_prop_id] = [
                    conn.dump(include_instance_type=False) for conn in created_connection
                ]
            else:
                created_properties[dest_prop_id] = created_connection.dump(include_instance_type=False)
        elif isinstance(dm_prop, ViewCorePropertyResponse):
            try:
                created_value = convert_to_primary_property(
                    value,
                    dm_prop.type,
                    dm_prop.nullable if dm_prop.nullable is not None else True,
                )
                created_properties[dest_prop_id] = serialize_dms(created_value)
            except (ValueError, TypeError, NotImplementedError) as e:
                errors.append(f"Failed to convert property {source_prop_id!r} with value {value!r}: {e!s}")
        # Else reverse direct relation, which we assume is handled in the other direction and thus ignore here.

    return ConversionResult(container_properties=created_properties, edges=edges, errors=errors)


def convert_edges(
    edge_targets_by_type_and_direction: dict[tuple[NodeId, Literal["outwards", "inwards"]], list[EdgeOtherSide]],
    mapping: ViewToViewMapping,
    destination_properties: dict[str, ViewResponseProperty],
    source_id: NodeId,
    connection_creator: ConnectionCreator,
    source_view_id: ViewId,
) -> ConversionResult:
    created_properties: dict[str, JsonValue] = {}
    new_edges: list[EdgeRequest] = []
    errors: list[str] = []
    for prop_id, source_edge_def in connection_creator.edges(source_view_id).items():
        edge_targets = edge_targets_by_type_and_direction.get((source_edge_def.type, source_edge_def.direction), [])
        if not edge_targets:
            continue

        dest_prop_id = mapping.get_destination_property(prop_id)
        if not dest_prop_id or dest_prop_id not in destination_properties:
            # Already captured as missing instance property in 'conver_container_properties', so we can just ignore it here.
            continue

        dm_prop = destination_properties[dest_prop_id]

        if isinstance(dm_prop, ViewCorePropertyResponse) and isinstance(dm_prop.type, DirectNodeRelation):
            try:
                created_connection, issues = connection_creator.create_direct_relation_from_edges(
                    edge_targets,
                    dm_prop.type,
                    prop_id,
                    source_view_id,
                )
            except ValueError as e:
                errors.append(
                    f"Failed to create direct relation for edge property {prop_id!r} with targets {[target.other_side.dump() for target in edge_targets]!r}: {e!s}"
                )
                continue
            errors.extend(issues)
            if isinstance(created_connection, list):
                created_properties[dest_prop_id] = [
                    conn.dump(include_instance_type=False) for conn in created_connection
                ]
            else:
                created_properties[dest_prop_id] = created_connection.dump(include_instance_type=False)
        elif isinstance(dm_prop, ViewCorePropertyResponse):
            # Todo: If json or text we can potentially convert to a string representation of the edge targets, but for now we just log an error.
            errors.append(f"Cannot map edge property {prop_id!r} to non-connection property {dm_prop.type.type!s}.")
        elif isinstance(dm_prop, EdgeProperty):
            try:
                created_edges, issues = connection_creator.create_edges_from_edges(
                    edge_targets,
                    dm_prop,
                    prop_id,
                    source_view_id,
                    source_id,
                )
            except ValueError as e:
                errors.append(
                    f"Failed to create edges for edge property {prop_id!r} with targets {[target.other_side.dump() for target in edge_targets]!r}: {e!s}"
                )
                continue
            errors.extend(issues)
            new_edges.extend(created_edges)
        # else reverse direct relation, which we assume is handled in the other direction and thus ignore here.

    return ConversionResult(container_properties=created_properties, edges=new_edges, errors=errors)
