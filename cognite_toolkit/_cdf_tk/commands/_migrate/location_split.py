"""Helpers for splitting shared legacy Infield instance spaces into per-location CDM spaces."""

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import EdgeTypeId, ExternalId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.commands._migrate.apm_source_data_mappings import get_first_instance_space
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    EdgeOtherSide,
    InstanceMappingError,
    LocationSplitInstanceIdMapper,
)
from cognite_toolkit._cdf_tk.dataio.logger import Severity
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name

CDM_SPACE_SUFFIX = "_cdm"
CFG_SPACE_SUFFIX = "_cfg"

_APM_SPACE = "cdf_apm"
TEMPLATE_VIEW = ViewId(space=_APM_SPACE, external_id="Template", version="v8")
CHECKLIST_VIEW = ViewId(space=_APM_SPACE, external_id="Checklist", version="v7")
OBSERVATION_VIEW = ViewId(space=_APM_SPACE, external_id="Observation", version="v5")
TEMPLATE_ITEM_VIEW = ViewId(space=_APM_SPACE, external_id="TemplateItem", version="v7")
CHECKLIST_ITEM_VIEW = ViewId(space=_APM_SPACE, external_id="ChecklistItem", version="v7")
SCHEDULE_VIEW = ViewId(space=_APM_SPACE, external_id="Schedule", version="v4")
CONDITIONAL_ACTION_VIEW = ViewId(space=_APM_SPACE, external_id="ConditionalAction", version="v1")
MEASUREMENT_VIEW = ViewId(space=_APM_SPACE, external_id="MeasurementReading", version="v4")
CONDITION_VIEW = ViewId(space=_APM_SPACE, external_id="Condition", version="v1")
ACTION_VIEW = ViewId(space=_APM_SPACE, external_id="Action", version="v1")

REFERENCE_TEMPLATE_ITEMS_EDGE = EdgeTypeId(
    type=NodeId(space=_APM_SPACE, external_id="referenceTemplateItems"), direction="inwards"
)
REFERENCE_CHECKLIST_ITEMS_EDGE = EdgeTypeId(
    type=NodeId(space=_APM_SPACE, external_id="referenceChecklistItems"), direction="inwards"
)
REFERENCE_MEASUREMENTS_EDGE = EdgeTypeId(
    type=NodeId(space=_APM_SPACE, external_id="referenceMeasurements"), direction="inwards"
)

# App-data views resolved directly from their own ``rootLocation`` property.
APP_DATA_ROOT_LOCATION_VIEWS = (TEMPLATE_VIEW, CHECKLIST_VIEW, OBSERVATION_VIEW)
# App-data views resolved by inheriting the target space of a parent found via an inbound edge.
APP_DATA_PARENT_EDGE_BY_VIEW: Mapping[ViewId, EdgeTypeId] = {
    TEMPLATE_ITEM_VIEW: REFERENCE_TEMPLATE_ITEMS_EDGE,
    CHECKLIST_ITEM_VIEW: REFERENCE_CHECKLIST_ITEMS_EDGE,
    MEASUREMENT_VIEW: REFERENCE_MEASUREMENTS_EDGE,
}
# App-data views resolved by inheriting the target space of a parent referenced via a direct relation
# property on the node itself.
APP_DATA_PARENT_PROPERTY_BY_VIEW: Mapping[ViewId, str] = {
    CONDITIONAL_ACTION_VIEW: "parentObject",
    CONDITION_VIEW: "conditionalAction",
    ACTION_VIEW: "conditionalActions",
}

LocationSplitKind = Literal["app_data", "source_data"]


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
        source_space=source_space,
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
        source_space=source_space,
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
    source_space: str,
    classic_root_assets: set[str],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    target_kind: LocationSplitKind,
) -> dict[str, str]:
    target_by_root_asset: dict[str, str] = {}
    for classic_root_asset in sorted(classic_root_assets):
        migrated_root = client.migration.lookup.assets(external_id=classic_root_asset)
        if migrated_root is None:
            continue
        target_space = _find_cdm_target_space(cdm_configs, migrated_root, target_kind=target_kind)
        if target_space is None:
            continue
        target_by_root_asset[classic_root_asset] = target_space
    space_kind = "appInstanceSpace" if target_kind == "app_data" else "source-data instance space"
    if not target_by_root_asset:
        raise ToolkitMigrationError(
            f"Legacy instance space {source_space!r} is shared by {len(classic_root_assets)} root location(s) "
            f"({', '.join(sorted(classic_root_assets))}), but Toolkit could not resolve a deployed CDM "
            f"{space_kind} for any of them. Ensure 'cdf migrate infield-configs' has been run and at least one "
            "location config sharing this space has been deployed."
        )
    # If two or more of the root locations that ARE deployed resolve to the same target space, that is a
    # genuine misconfiguration (it defeats the purpose of splitting a shared space) and should fail loudly
    root_assets_by_target_space: dict[str, list[str]] = defaultdict(list)
    for classic_root_asset, target_space in target_by_root_asset.items():
        root_assets_by_target_space[target_space].append(classic_root_asset)
    duplicated = {
        target_space: root_assets
        for target_space, root_assets in root_assets_by_target_space.items()
        if len(root_assets) > 1
    }
    if duplicated:
        conflicts = "; ".join(
            f"{target_space!r} is shared by {', '.join(sorted(root_assets))}"
            for target_space, root_assets in sorted(duplicated.items())
        )
        raise ToolkitMigrationError(
            f"Legacy instance space {source_space!r} is shared by multiple root locations, but (some of) the deployed "
            f"CDM location configs use the same {space_kind}: {conflicts}. Each root location "
            f"sharing this space must be configured with its own unique {space_kind}."
        )
    return target_by_root_asset


def _find_cdm_target_space(
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    migrated_root: NodeId,
    *,
    target_kind: LocationSplitKind,
) -> str | None:
    matching_configs = [
        config
        for config in cdm_configs
        if config.data_storage is not None
        and config.data_storage.root_location is not None
        and config.data_storage.root_location.get("space") == migrated_root.space
        and config.data_storage.root_location.get("externalId") == migrated_root.external_id
    ]
    if not matching_configs:
        return None
    if len(matching_configs) > 1:
        raise ToolkitMigrationError(
            f"Cannot proceed with migration: Found {len(matching_configs)} deployed InField CDM location configs "
            f"({', '.join(sorted(config.external_id for config in matching_configs))}) with the same root "
            f"location {migrated_root!s}. Each root location must have exactly one deployed CDM location config "
            "in order to use this migration tool to non-ambiguously assign target spaces."
        )
    config = matching_configs[0]
    data_storage = config.data_storage
    assert data_storage is not None
    if target_kind == "app_data":
        return data_storage.app_instance_space
    return get_first_instance_space(config.data_filters, "maintenanceOrder")


def root_internal_id_to_target_space(client: ToolkitClient, target_by_root_asset: Mapping[str, str]) -> dict[int, str]:
    """Map classic root asset internal ID -> target space, for resolving instances that only carry a
    classic asset reference (e.g. APM_SourceData Notifications), rather than a direct/edge lineage link.
    """
    root_assets = client.tool.assets.retrieve(
        ExternalId.from_external_ids(target_by_root_asset.keys()), ignore_unknown_ids=True
    )
    root_id_to_target: dict[int, str] = {}
    for asset in root_assets:
        if asset.external_id is None or asset.external_id not in target_by_root_asset:
            continue
        root_id_to_target[asset.id] = target_by_root_asset[asset.external_id]
    return root_id_to_target


class TargetSpaceResolver(ABC):
    """Resolves target spaces for nodes of one view that need a resolution mechanism beyond a simple
    ``rootLocation`` property, inbound edge, or direct-relation property to an already-migrated instance
    (e.g. a lookup against classic Assets, as needed for APM_SourceData Notifications).
    """

    def prepare_page(self, nodes: Sequence[NodeResponse]) -> None:
        """Optionally batch-prefetch data needed to resolve every node in a page, in as few round trips
        as possible. The default implementation does nothing.
        """

    @abstractmethod
    def resolve(self, node: NodeResponse) -> str:
        raise NotImplementedError


class AssetExternalIdTargetSpaceResolver(TargetSpaceResolver):
    """Resolves a node's target space via a classic asset reference on the node itself (e.g. Notification's
    ``assetExternalId``), by looking up that asset's root asset.

    Root asset -> target space is already known upfront (``root_id_to_target_space``, derived from InField
    location configs, not from a full instance pre-scan). Resolving a node's own asset reference to its root
    asset is the only per-instance lookup needed, and it is batched per page via ``prepare_page`` rather than
    issued once per node.
    """

    def __init__(
        self,
        client: ToolkitClient,
        view_id: ViewId,
        property_id: str,
        root_id_to_target_space: Mapping[int, str],
    ) -> None:
        self._client = client
        self._view_id = view_id
        self._property_id = property_id
        self._root_id_to_target_space = root_id_to_target_space
        self._root_id_by_asset_external_id: dict[str, int | None] = {}

    def prepare_page(self, nodes: Sequence[NodeResponse]) -> None:
        asset_external_ids = {
            asset_external_id
            for node in nodes
            if (asset_external_id := _as_external_id(_get_view_property(node, self._view_id, self._property_id)))
            is not None
            and asset_external_id not in self._root_id_by_asset_external_id
        }
        if not asset_external_ids:
            return
        assets = self._client.tool.assets.retrieve(
            ExternalId.from_external_ids(asset_external_ids), ignore_unknown_ids=True
        )
        found_by_external_id = {asset.external_id: asset.root_id for asset in assets if asset.external_id is not None}
        for asset_external_id in asset_external_ids:
            self._root_id_by_asset_external_id[asset_external_id] = found_by_external_id.get(asset_external_id)

    def resolve(self, node: NodeResponse) -> str:
        asset_external_id = _as_external_id(_get_view_property(node, self._view_id, self._property_id))
        if asset_external_id is None:
            raise InstanceMappingError(f"{node.as_id()} is missing {self._property_id}.", severity=Severity.failure)
        root_id = self._root_id_by_asset_external_id.get(asset_external_id)
        if root_id is None:
            raise InstanceMappingError(
                f"{node.as_id()}: asset {asset_external_id!r} referenced via {self._property_id} was not found.",
                severity=Severity.failure,
            )
        target_space = self._root_id_to_target_space.get(root_id)
        if target_space is None:
            raise InstanceMappingError(
                f"{node.as_id()}: asset {asset_external_id!r} root asset does not match a location sharing this "
                "source space.",
                severity=Severity.failure,
            )
        return target_space


def resolve_target_space_via_root_location(
    node: NodeResponse, view_id: ViewId, target_by_root_asset: Mapping[str, str]
) -> str:
    """Resolve a node's target space directly from its own ``rootLocation`` property.

    Used for the "tier 0" app-data/source-data views (Template, Checklist, Observation, Activity) that
    carry a direct pointer to their root location, so no lineage lookup is needed.
    """
    root_location = _as_external_id(_get_view_property(node, view_id, "rootLocation"))
    if root_location is None:
        raise InstanceMappingError(f"{node.as_id()} is missing rootLocation.", severity=Severity.failure)
    target_space = target_by_root_asset.get(root_location)
    if target_space is None:
        raise InstanceMappingError(
            f"{node.as_id()} has rootLocation {root_location!r}, which does not match a location sharing this "
            "legacy instance space.",
            severity=Severity.failure,
        )
    return target_space


def resolve_target_space_via_parent_edge(
    node: NodeResponse,
    parent_edge_type: EdgeTypeId,
    other_side_by_edge_type_and_direction: Mapping[EdgeTypeId, Sequence[EdgeOtherSide]],
    instance_id_mapper: LocationSplitInstanceIdMapper,
) -> str:
    """Resolve a node's target space by inheriting it from a parent found via an inbound edge.

    The parent's own target space must already be resolvable, either because it was migrated earlier in
    this same run (registered in-memory), or because it was migrated in a previous run and is tagged in
    the hidden relocation view.
    """
    parents = other_side_by_edge_type_and_direction.get(parent_edge_type, [])
    if not parents:
        raise InstanceMappingError(
            f"{node.as_id()} has no inbound {parent_edge_type!s} edge to a parent.", severity=Severity.failure
        )
    target_spaces = {
        target_space
        for parent in parents
        if (target_space := instance_id_mapper.resolve_target_space(parent.other_side.external_id)) is not None
    }
    if len(target_spaces) != 1:
        reason = "unresolved" if not target_spaces else "disagree on target space"
        raise InstanceMappingError(
            f"{node.as_id()}: parent(s) via {parent_edge_type!s} are {reason}.", severity=Severity.failure
        )
    return next(iter(target_spaces))


def resolve_target_space_via_parent_property(
    node: NodeResponse,
    view_id: ViewId,
    parent_property: str,
    instance_id_mapper: LocationSplitInstanceIdMapper,
) -> str:
    """Resolve a node's target space by inheriting it from a parent referenced via a direct relation
    property on the node itself (e.g. ``parentObject``, ``parentActivityId``).
    """
    parent_external_id = _as_external_id(_get_view_property(node, view_id, parent_property))
    if parent_external_id is None:
        raise InstanceMappingError(f"{node.as_id()} is missing {parent_property}.", severity=Severity.failure)
    target_space = instance_id_mapper.resolve_target_space(parent_external_id)
    if target_space is None:
        raise InstanceMappingError(
            f"{node.as_id()}: parent {parent_external_id!r} via {parent_property} is unresolved.",
            severity=Severity.failure,
        )
    return target_space


def _get_view_property(node: NodeResponse, view_id: ViewId, property_id: str) -> Any:
    properties = (node.properties or {}).get(view_id)
    if not isinstance(properties, dict):
        return None
    return properties.get(property_id)


def _as_external_id(value: Any) -> str | None:
    """Extract the external ID referenced by a property value, handling both representations used here.

    Some properties (e.g. ``rootLocation``, ``parentObject``) are genuine direct relations, which
    come back from ``_get_view_property`` as a raw ``{"space": ..., "externalId": ...}`` dict --
    pydantic keeps generic view properties as plain ``JsonValue``, it doesn't upgrade them to a
    parsed ``NodeId``. Others (e.g. ``parentActivityId``, ``assetExternalId`` on the APM_SourceData
    model) are plain text external-ID properties, not direct relations, so they come back as a
    ``str`` directly.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        external_id = value.get("externalId")
        return external_id if isinstance(external_id, str) else None
    return None
