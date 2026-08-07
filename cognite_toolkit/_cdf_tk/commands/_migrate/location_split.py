"""Helpers for splitting shared legacy Infield instance spaces into per-location CDM spaces."""

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import EdgeResponse, NodeResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.commands._migrate.apm_source_data_mappings import get_first_instance_space
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name

CDM_SPACE_SUFFIX = "_cdm"
CFG_SPACE_SUFFIX = "_cfg"

_APM_SPACE = "cdf_apm"
_TEMPLATE_VIEW = ViewId(space=_APM_SPACE, external_id="Template", version="v8")
_CHECKLIST_VIEW = ViewId(space=_APM_SPACE, external_id="Checklist", version="v7")
_OBSERVATION_VIEW = ViewId(space=_APM_SPACE, external_id="Observation", version="v5")
_TEMPLATE_ITEM_VIEW = ViewId(space=_APM_SPACE, external_id="TemplateItem", version="v7")
_CHECKLIST_ITEM_VIEW = ViewId(space=_APM_SPACE, external_id="ChecklistItem", version="v7")
_SCHEDULE_VIEW = ViewId(space=_APM_SPACE, external_id="Schedule", version="v4")
_CONDITIONAL_ACTION_VIEW = ViewId(space=_APM_SPACE, external_id="ConditionalAction", version="v1")
_MEASUREMENT_VIEW = ViewId(space=_APM_SPACE, external_id="MeasurementReading", version="v4")
_CONDITION_VIEW = ViewId(space=_APM_SPACE, external_id="Condition", version="v1")
_ACTION_VIEW = ViewId(space=_APM_SPACE, external_id="Action", version="v1")

_REFERENCE_TEMPLATE_ITEMS = "referenceTemplateItems"
_REFERENCE_CHECKLIST_ITEMS = "referenceChecklistItems"
_REFERENCE_SCHEDULES = "referenceSchedules"
_REFERENCE_MEASUREMENTS = "referenceMeasurements"

LocationSplitKind = Literal["app_data", "source_data"]


@dataclass
class LocationSplitAssignment:
    external_id: str
    view_external_id: str
    target_space: str


@dataclass
class LocationSplitOrphan:
    external_id: str
    view_external_id: str
    reason: str


@dataclass
class LocationSplitResolution:
    assignments: list[LocationSplitAssignment] = field(default_factory=list)
    orphans: list[LocationSplitOrphan] = field(default_factory=list)

    @property
    def target_space_by_external_id(self) -> dict[str, str]:
        return {assignment.external_id: assignment.target_space for assignment in self.assignments}

    @property
    def target_spaces(self) -> set[str]:
        return {assignment.target_space for assignment in self.assignments}

    @property
    def orphan_reason_by_external_id(self) -> dict[str, str]:
        return {orphan.external_id: orphan.reason for orphan in self.orphans}


def find_shared_legacy_instance_spaces(apm_configs: Sequence[APMConfigResponse]) -> set[str]:
    """Return legacy instance spaces used by more than one root location.

    A space is shared if it appears as ``appDataInstanceSpace`` for more than one root location,
    or as ``sourceDataInstanceSpace`` for more than one root location. The same space used as both
    app and source data for a single location does not count as shared.

    Only root locations with an ``assetExternalId`` are counted. That field is required both to
    create a CDM location config and to disambiguate shared-space names, so roots without it cannot
    participate in a split.
    """
    locations_by_app_space: dict[str, set[str]] = defaultdict(set)
    locations_by_source_space: dict[str, set[str]] = defaultdict(set)
    for config in apm_configs:
        if not config.feature_configuration:
            continue
        for root in config.feature_configuration.root_location_configurations or []:
            if not root.asset_external_id:
                continue
            if root.app_data_instance_space:
                locations_by_app_space[root.app_data_instance_space].add(root.asset_external_id)
            if root.source_data_instance_space:
                locations_by_source_space[root.source_data_instance_space].add(root.asset_external_id)

    shared: set[str] = set()
    for space, locations in locations_by_app_space.items():
        if len(locations) > 1:
            shared.add(space)
    for space, locations in locations_by_source_space.items():
        if len(locations) > 1:
            shared.add(space)
    return shared


def build_infield_instance_space_name(
    legacy_space: str,
    suffix: str,
    *,
    asset_external_id: str | None = None,
    shared: bool = False,
) -> str:
    """Build a CDM / config instance space name from a legacy Infield instance space.

    When ``shared`` is False, returns ``{legacy_space}{suffix}`` (subject to DMS space-name
    sanitization). When ``shared`` is True, inserts the root asset external ID:
    ``{legacy_space}_{asset_external_id}{suffix}``, truncated and hashed if needed to stay within
    the 43-character DMS space ID limit.
    """
    if shared:
        if not asset_external_id:
            raise ValueError("asset_external_id is required when building a shared-location instance space name")
        candidate = f"{legacy_space}_{asset_external_id}{suffix}"
    else:
        candidate = f"{legacy_space}{suffix}"
    return fix_invalid_space_name(candidate)


def resolve_app_data_location_split(
    client: ToolkitClient,
    *,
    source_space: str,
    apm_configs: Sequence[APMConfigResponse],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
) -> LocationSplitResolution:
    """Resolve per-instance target spaces for shared ``appDataInstanceSpace`` app data."""
    target_by_root_asset = build_app_data_target_by_root_asset(
        client, source_space=source_space, apm_configs=apm_configs, cdm_configs=cdm_configs
    )
    return _resolve_app_data_cascade(client, source_space=source_space, target_by_root_asset=target_by_root_asset)


def resolve_source_data_location_split(
    client: ToolkitClient,
    *,
    source_space: str,
    apm_configs: Sequence[APMConfigResponse],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    activity_view: ViewId,
    operation_view: ViewId,
    notification_view: ViewId,
) -> LocationSplitResolution:
    """Resolve per-instance target spaces for shared ``sourceDataInstanceSpace`` source data."""
    target_by_root_asset = build_source_data_target_by_root_asset(
        client, source_space=source_space, apm_configs=apm_configs, cdm_configs=cdm_configs
    )
    return _resolve_source_data_cascade(
        client,
        source_space=source_space,
        target_by_root_asset=target_by_root_asset,
        activity_view=activity_view,
        operation_view=operation_view,
        notification_view=notification_view,
    )


def build_app_data_target_by_root_asset(
    client: ToolkitClient,
    *,
    source_space: str,
    apm_configs: Sequence[APMConfigResponse],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
) -> dict[str, str]:
    """Map classic root asset external ID -> CDM ``appInstanceSpace`` for locations using ``source_space``."""
    classic_root_assets = _classic_root_assets_for_legacy_space(
        apm_configs, source_space=source_space, space_attr="app_data_instance_space"
    )
    return _target_spaces_for_root_assets(
        client,
        classic_root_assets=classic_root_assets,
        cdm_configs=cdm_configs,
        target_kind="app_data",
    )


def build_source_data_target_by_root_asset(
    client: ToolkitClient,
    *,
    source_space: str,
    apm_configs: Sequence[APMConfigResponse],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
) -> dict[str, str]:
    """Map classic root asset external ID -> CDM source-data instance space for locations using ``source_space``."""
    classic_root_assets = _classic_root_assets_for_legacy_space(
        apm_configs, source_space=source_space, space_attr="source_data_instance_space"
    )
    return _target_spaces_for_root_assets(
        client,
        classic_root_assets=classic_root_assets,
        cdm_configs=cdm_configs,
        target_kind="source_data",
    )


def _classic_root_assets_for_legacy_space(
    apm_configs: Sequence[APMConfigResponse],
    *,
    source_space: str,
    space_attr: Literal["app_data_instance_space", "source_data_instance_space"],
) -> set[str]:
    classic_root_assets: set[str] = set()
    for config in apm_configs:
        if not config.feature_configuration:
            continue
        for root in config.feature_configuration.root_location_configurations or []:
            if not root.asset_external_id:
                continue
            if getattr(root, space_attr) == source_space:
                classic_root_assets.add(root.asset_external_id)
    if len(classic_root_assets) < 2:
        raise ToolkitMigrationError(
            f"Expected at least two root locations using legacy instance space {source_space!r} for a location "
            f"split, found {len(classic_root_assets)}."
        )
    return classic_root_assets


def _target_spaces_for_root_assets(
    client: ToolkitClient,
    *,
    classic_root_assets: set[str],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    target_kind: LocationSplitKind,
) -> dict[str, str]:
    target_by_root_asset: dict[str, str] = {}
    missing: list[str] = []
    for classic_root_asset in sorted(classic_root_assets):
        migrated_root = client.migration.lookup.assets(external_id=classic_root_asset)
        if migrated_root is None:
            missing.append(classic_root_asset)
            continue
        target_space = _find_cdm_target_space(cdm_configs, migrated_root, target_kind=target_kind)
        if target_space is None:
            missing.append(classic_root_asset)
            continue
        target_by_root_asset[classic_root_asset] = target_space
    if missing:
        raise ToolkitMigrationError(
            f"Could not resolve CDM target spaces for root asset(s) {', '.join(missing)}. "
            "Ensure 'cdf migrate infield-configs' has been run and the generated location configs and "
            "per-location instance spaces have been deployed."
        )
    if len(set(target_by_root_asset.values())) < 2:
        raise ToolkitMigrationError(
            "Location split requires at least two distinct CDM target spaces, but the deployed location "
            f"configs resolve to: {sorted(set(target_by_root_asset.values()))}."
        )
    return target_by_root_asset


def _find_cdm_target_space(
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    migrated_root: NodeId,
    *,
    target_kind: LocationSplitKind,
) -> str | None:
    for config in cdm_configs:
        if config.data_storage is None or config.data_storage.root_location is None:
            continue
        root_location = config.data_storage.root_location
        if (
            root_location.get("space") != migrated_root.space
            or root_location.get("externalId") != migrated_root.external_id
        ):
            continue
        if target_kind == "app_data":
            return config.data_storage.app_instance_space
        return get_first_instance_space(config.data_filters, "maintenanceOrder")
    return None


def _resolve_app_data_cascade(
    client: ToolkitClient,
    *,
    source_space: str,
    target_by_root_asset: Mapping[str, str],
) -> LocationSplitResolution:
    resolution = LocationSplitResolution()
    target_by_external_id: dict[str, str] = {}

    for view_id in (_TEMPLATE_VIEW, _CHECKLIST_VIEW, _OBSERVATION_VIEW):
        _resolve_tier0_root_location_nodes(
            client,
            source_space=source_space,
            view_id=view_id,
            target_by_root_asset=target_by_root_asset,
            target_by_external_id=target_by_external_id,
            resolution=resolution,
        )

    for edge_type_external_id, child_view_id in (
        (_REFERENCE_TEMPLATE_ITEMS, _TEMPLATE_ITEM_VIEW),
        (_REFERENCE_CHECKLIST_ITEMS, _CHECKLIST_ITEM_VIEW),
        (_REFERENCE_SCHEDULES, _SCHEDULE_VIEW),
    ):
        _resolve_children_via_outbound_edges(
            client,
            source_space=source_space,
            edge_type_external_id=edge_type_external_id,
            child_view_id=child_view_id,
            target_by_external_id=target_by_external_id,
            resolution=resolution,
        )

    _resolve_measurements_via_inbound_edges(
        client,
        source_space=source_space,
        target_by_external_id=target_by_external_id,
        resolution=resolution,
    )

    for view_id, parent_property in (
        (_CONDITIONAL_ACTION_VIEW, "parentObject"),
        (_CONDITION_VIEW, "conditionalAction"),
        (_ACTION_VIEW, "conditionalActions"),
    ):
        _resolve_via_parent_direct_relation(
            client,
            source_space=source_space,
            view_id=view_id,
            parent_property=parent_property,
            target_by_external_id=target_by_external_id,
            resolution=resolution,
        )
    return resolution


def _resolve_source_data_cascade(
    client: ToolkitClient,
    *,
    source_space: str,
    target_by_root_asset: Mapping[str, str],
    activity_view: ViewId,
    operation_view: ViewId,
    notification_view: ViewId,
) -> LocationSplitResolution:
    resolution = LocationSplitResolution()
    target_by_external_id: dict[str, str] = {}

    _resolve_tier0_root_location_nodes(
        client,
        source_space=source_space,
        view_id=activity_view,
        target_by_root_asset=target_by_root_asset,
        target_by_external_id=target_by_external_id,
        resolution=resolution,
    )
    _resolve_via_parent_direct_relation(
        client,
        source_space=source_space,
        view_id=operation_view,
        parent_property="parentActivityId",
        target_by_external_id=target_by_external_id,
        resolution=resolution,
    )

    root_id_to_target = _root_internal_id_to_target_space(client, target_by_root_asset)
    notification_nodes = _list_nodes(client, notification_view, source_space)
    asset_external_ids = {
        asset_external_id
        for node in notification_nodes
        if (asset_external_id := _as_external_id(_get_view_property(node, notification_view, "assetExternalId")))
        is not None
    }
    asset_by_external_id = {
        asset.external_id: asset
        for asset in client.tool.assets.retrieve(
            ExternalId.from_external_ids(asset_external_ids), ignore_unknown_ids=True
        )
        if asset.external_id is not None
    }
    for node in notification_nodes:
        asset_external_id = _as_external_id(_get_view_property(node, notification_view, "assetExternalId"))
        if asset_external_id is None:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=node.external_id,
                    view_external_id=notification_view.external_id,
                    reason="missing assetExternalId",
                )
            )
            continue
        asset = asset_by_external_id.get(asset_external_id)
        if asset is None:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=node.external_id,
                    view_external_id=notification_view.external_id,
                    reason=f"asset {asset_external_id!r} not found",
                )
            )
            continue
        target_space = root_id_to_target.get(asset.root_id)
        if target_space is None:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=node.external_id,
                    view_external_id=notification_view.external_id,
                    reason=f"asset {asset_external_id!r} root_id does not match a location sharing this source space",
                )
            )
            continue
        _assign(resolution, target_by_external_id, node.external_id, notification_view.external_id, target_space)

    return resolution


def _resolve_tier0_root_location_nodes(
    client: ToolkitClient,
    *,
    source_space: str,
    view_id: ViewId,
    target_by_root_asset: Mapping[str, str],
    target_by_external_id: dict[str, str],
    resolution: LocationSplitResolution,
) -> None:
    for node in _list_nodes(client, view_id, source_space):
        root_location = _as_external_id(_get_view_property(node, view_id, "rootLocation"))
        if root_location is None:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=node.external_id,
                    view_external_id=view_id.external_id,
                    reason="missing rootLocation",
                )
            )
            continue
        target_space = target_by_root_asset.get(root_location)
        if target_space is None:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=node.external_id,
                    view_external_id=view_id.external_id,
                    reason=f"rootLocation {root_location!r} does not match a location sharing this source space",
                )
            )
            continue
        _assign(resolution, target_by_external_id, node.external_id, view_id.external_id, target_space)


def _resolve_children_via_outbound_edges(
    client: ToolkitClient,
    *,
    source_space: str,
    edge_type_external_id: str,
    child_view_id: ViewId,
    target_by_external_id: dict[str, str],
    resolution: LocationSplitResolution,
) -> None:
    child_external_ids = {node.external_id for node in _list_nodes(client, child_view_id, source_space)}
    parent_by_child: dict[str, list[str]] = defaultdict(list)
    for edge in _list_edges(client, source_space=source_space, edge_type_external_id=edge_type_external_id):
        child_external_id = edge.end_node.external_id
        if child_external_id not in child_external_ids:
            continue
        parent_by_child[child_external_id].append(edge.start_node.external_id)

    for child_external_id in sorted(child_external_ids):
        parents = parent_by_child.get(child_external_id, [])
        if not parents:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=child_external_id,
                    view_external_id=child_view_id.external_id,
                    reason=f"missing inbound {edge_type_external_id} edge to a resolved parent",
                )
            )
            continue
        parent_spaces = {target_by_external_id[parent] for parent in parents if parent in target_by_external_id}
        if len(parent_spaces) != 1:
            unresolved_parents = [parent for parent in parents if parent not in target_by_external_id]
            if unresolved_parents and not parent_spaces:
                reason = f"parent(s) {', '.join(unresolved_parents)} were not resolved"
            else:
                reason = f"conflicting parent target spaces via {edge_type_external_id}"
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=child_external_id,
                    view_external_id=child_view_id.external_id,
                    reason=reason,
                )
            )
            continue
        _assign(
            resolution,
            target_by_external_id,
            child_external_id,
            child_view_id.external_id,
            next(iter(parent_spaces)),
        )


def _resolve_measurements_via_inbound_edges(
    client: ToolkitClient,
    *,
    source_space: str,
    target_by_external_id: dict[str, str],
    resolution: LocationSplitResolution,
) -> None:
    measurement_external_ids = {node.external_id for node in _list_nodes(client, _MEASUREMENT_VIEW, source_space)}
    parents_by_measurement: dict[str, list[str]] = defaultdict(list)
    for edge in _list_edges(client, source_space=source_space, edge_type_external_id=_REFERENCE_MEASUREMENTS):
        measurement_external_id = edge.end_node.external_id
        if measurement_external_id not in measurement_external_ids:
            continue
        parents_by_measurement[measurement_external_id].append(edge.start_node.external_id)

    for measurement_external_id in sorted(measurement_external_ids):
        parents = parents_by_measurement.get(measurement_external_id, [])
        if not parents:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=measurement_external_id,
                    view_external_id=_MEASUREMENT_VIEW.external_id,
                    reason="missing inbound referenceMeasurements edge",
                )
            )
            continue
        parent_spaces = {target_by_external_id[parent] for parent in parents if parent in target_by_external_id}
        if len(parents) != 1 or len(parent_spaces) != 1:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=measurement_external_id,
                    view_external_id=_MEASUREMENT_VIEW.external_id,
                    reason="conflicting or unresolved inbound referenceMeasurements edges",
                )
            )
            continue
        _assign(
            resolution,
            target_by_external_id,
            measurement_external_id,
            _MEASUREMENT_VIEW.external_id,
            next(iter(parent_spaces)),
        )


def _resolve_via_parent_direct_relation(
    client: ToolkitClient,
    *,
    source_space: str,
    view_id: ViewId,
    parent_property: str,
    target_by_external_id: dict[str, str],
    resolution: LocationSplitResolution,
) -> None:
    for node in _list_nodes(client, view_id, source_space):
        parent_external_id = _as_external_id(_get_view_property(node, view_id, parent_property))
        if parent_external_id is None:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=node.external_id,
                    view_external_id=view_id.external_id,
                    reason=f"missing {parent_property}",
                )
            )
            continue
        target_space = target_by_external_id.get(parent_external_id)
        if target_space is None:
            resolution.orphans.append(
                LocationSplitOrphan(
                    external_id=node.external_id,
                    view_external_id=view_id.external_id,
                    reason=f"parent {parent_external_id!r} via {parent_property} was not resolved",
                )
            )
            continue
        _assign(resolution, target_by_external_id, node.external_id, view_id.external_id, target_space)


def _root_internal_id_to_target_space(client: ToolkitClient, target_by_root_asset: Mapping[str, str]) -> dict[int, str]:
    root_assets = client.tool.assets.retrieve(
        ExternalId.from_external_ids(target_by_root_asset.keys()), ignore_unknown_ids=True
    )
    root_id_to_target: dict[int, str] = {}
    for asset in root_assets:
        if asset.external_id is None or asset.external_id not in target_by_root_asset:
            continue
        root_id_to_target[asset.id] = target_by_root_asset[asset.external_id]
    return root_id_to_target


def _list_nodes(client: ToolkitClient, view_id: ViewId, source_space: str) -> list[NodeResponse]:
    items = client.tool.instances.list(
        InstanceFilter(instance_type="node", source=view_id, space=[source_space]),
        limit=None,
        endpoint="query",
    )
    return [item for item in items if isinstance(item, NodeResponse)]


def _list_edges(client: ToolkitClient, *, source_space: str, edge_type_external_id: str) -> list[EdgeResponse]:
    items = client.tool.instances.list(
        InstanceFilter(
            instance_type="edge",
            filter={
                "and": [
                    {"equals": {"property": ["edge", "space"], "value": source_space}},
                    {
                        "equals": {
                            "property": ["edge", "type"],
                            "value": {"space": _APM_SPACE, "externalId": edge_type_external_id},
                        }
                    },
                ]
            },
        ),
        limit=None,
        endpoint="query",
    )
    return [item for item in items if isinstance(item, EdgeResponse)]


def _get_view_property(node: NodeResponse, view_id: ViewId, property_id: str) -> Any:
    properties = (node.properties or {}).get(view_id)
    if not isinstance(properties, dict):
        return None
    return properties.get(property_id)


def _as_external_id(value: Any) -> str | None:
    if isinstance(value, str):
        return value
    if isinstance(value, NodeId):
        return value.external_id
    if isinstance(value, dict):
        external_id = value.get("externalId")
        return external_id if isinstance(external_id, str) else None
    return None


def _assign(
    resolution: LocationSplitResolution,
    target_by_external_id: dict[str, str],
    external_id: str,
    view_external_id: str,
    target_space: str,
) -> None:
    target_by_external_id[external_id] = target_space
    resolution.assignments.append(
        LocationSplitAssignment(external_id=external_id, view_external_id=view_external_id, target_space=target_space)
    )
