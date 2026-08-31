"""Helpers for splitting shared legacy Infield instance spaces into per-location CDM spaces."""

from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from typing import Any, Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import EdgeTypeId, ExternalId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse, RootLocationConfiguration
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.commands._migrate.apm_source_data_mappings import get_first_instance_space
from cognite_toolkit._cdf_tk.commands._migrate.conversion import (
    LocationSplitInstanceIdMapper,
    TargetSpaceResolutionError,
)
from cognite_toolkit._cdf_tk.dataio.logger import Severity
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name

CDM_SPACE_SUFFIX = "_cdm"
CFG_SPACE_SUFFIX = "_cfg"

LocationSplitKind = Literal["app_data", "source_data"]


def _iter_root_locations(
    apm_configs: Sequence[APMConfigResponse],
) -> Iterator[tuple[RootLocationConfiguration, str]]:
    for config in apm_configs:
        if not config.feature_configuration:
            continue
        for root in config.feature_configuration.root_location_configurations or []:
            if root.asset_external_id:
                yield root, root.asset_external_id


def _legacy_instance_space(root: RootLocationConfiguration, target_kind: LocationSplitKind) -> str | None:
    if target_kind == "app_data":
        return root.app_data_instance_space
    return root.source_data_instance_space


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
    for root, asset_external_id in _iter_root_locations(apm_configs):
        if root.app_data_instance_space:
            locations_by_app_space[root.app_data_instance_space].add(asset_external_id)
        if root.source_data_instance_space:
            locations_by_source_space[root.source_data_instance_space].add(asset_external_id)

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


def build_target_by_root_asset(
    client: ToolkitClient,
    *,
    source_space: str,
    apm_configs: Sequence[APMConfigResponse],
    cdm_configs: Sequence[InFieldCDMLocationConfigResponse],
    target_kind: LocationSplitKind,
) -> dict[str, str]:
    """Map classic root asset external ID -> CDM target space for locations using ``source_space``."""
    classic_root_assets: set[str] = set()
    for root, asset_external_id in _iter_root_locations(apm_configs):
        if _legacy_instance_space(root, target_kind) == source_space:
            classic_root_assets.add(asset_external_id)
    if len(classic_root_assets) < 2:
        raise ToolkitMigrationError(
            f"Expected at least two root locations using legacy instance space {source_space!r} for a location "
            f"split, found {len(classic_root_assets)}."
        )

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
    # For source-data migration: choose arbitrarily one of maintenanceOrder,
    # operation or notification's instance space as target space,
    # as they per the docs are expected to be the same space.
    return get_first_instance_space(config.data_filters, "maintenanceOrder")


_APM_SPACE = "cdf_apm"
_TEMPLATE_VIEW = ViewId(space=_APM_SPACE, external_id="Template", version="v8")
_CHECKLIST_VIEW = ViewId(space=_APM_SPACE, external_id="Checklist", version="v7")
_OBSERVATION_VIEW = ViewId(space=_APM_SPACE, external_id="Observation", version="v5")
_TEMPLATE_ITEM_VIEW = ViewId(space=_APM_SPACE, external_id="TemplateItem", version="v7")
_CHECKLIST_ITEM_VIEW = ViewId(space=_APM_SPACE, external_id="ChecklistItem", version="v7")
_CONDITIONAL_ACTION_VIEW = ViewId(space=_APM_SPACE, external_id="ConditionalAction", version="v1")
_MEASUREMENT_VIEW = ViewId(space=_APM_SPACE, external_id="MeasurementReading", version="v4")
_CONDITION_VIEW = ViewId(space=_APM_SPACE, external_id="Condition", version="v1")
_ACTION_VIEW = ViewId(space=_APM_SPACE, external_id="Action", version="v1")

COGNITE_SOLUTION_TAG_VIEW_ID = ViewId(space="cdf_apps_shared", external_id="CogniteSolutionTag", version="v1")

# Resolved from the node's own rootLocation.
APP_DATA_ROOT_LOCATION_VIEWS = (_TEMPLATE_VIEW, _CHECKLIST_VIEW, _OBSERVATION_VIEW)
# Child views that inherit target space from a parent tagged in InstanceSpaceRelocationSource.
# Inbound edges are read on the child /sync page so a newly added child is picked up with its parent edge.
APP_DATA_PARENT_EDGE_BY_VIEW: Mapping[ViewId, EdgeTypeId] = {
    _TEMPLATE_ITEM_VIEW: EdgeTypeId(
        type=NodeId(space=_APM_SPACE, external_id="referenceTemplateItems"), direction="inwards"
    ),
    _CHECKLIST_ITEM_VIEW: EdgeTypeId(
        type=NodeId(space=_APM_SPACE, external_id="referenceChecklistItems"), direction="inwards"
    ),
    _MEASUREMENT_VIEW: EdgeTypeId(
        type=NodeId(space=_APM_SPACE, external_id="referenceMeasurements"), direction="inwards"
    ),
}
APP_DATA_PARENT_EDGE_TYPES = frozenset(edge_type.type for edge_type in APP_DATA_PARENT_EDGE_BY_VIEW.values())
# Inherit target space from a parent referenced by a direct-relation property.
APP_DATA_PARENT_PROPERTY_BY_VIEW: Mapping[ViewId, str] = {
    _CONDITIONAL_ACTION_VIEW: "parentObject",
    _CONDITION_VIEW: "conditionalAction",
    _ACTION_VIEW: "conditionalActions",
}


def get_view_property(node: NodeResponse, view_id: ViewId, property_id: str) -> Any:
    properties = (node.properties or {}).get(view_id)
    if not isinstance(properties, dict):
        return None
    return properties.get(property_id)


def as_external_id(value: Any) -> str | None:
    """Direct relations are ``{space, externalId}`` dicts; some APM properties are plain strings."""
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        external_id = value.get("externalId")
        return external_id if isinstance(external_id, str) else None
    return None


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


class AssetExternalIdTargetSpaceResolver:
    """Resolve target space from a classic asset reference on the node (e.g. Notification.assetExternalId)."""

    def __init__(
        self,
        client: ToolkitClient,
        view_id: ViewId,
        property_id: str,
        target_by_root_asset: Mapping[str, str],
    ) -> None:
        self._client = client
        self._view_id = view_id
        self._property_id = property_id
        self._root_id_to_target_space = _root_internal_id_to_target_space(client, target_by_root_asset)
        self._root_id_by_asset_external_id: dict[str, int | None] = {}

    def prepare_page(self, nodes: Sequence[NodeResponse]) -> None:
        asset_external_ids: set[str] = set()
        for node in nodes:
            asset_external_id = as_external_id(get_view_property(node, self._view_id, self._property_id))
            if asset_external_id is not None and asset_external_id not in self._root_id_by_asset_external_id:
                asset_external_ids.add(asset_external_id)
        if not asset_external_ids:
            return
        assets = self._client.tool.assets.retrieve(
            ExternalId.from_external_ids(asset_external_ids), ignore_unknown_ids=True
        )
        found_by_external_id = {asset.external_id: asset.root_id for asset in assets if asset.external_id is not None}
        for asset_external_id in asset_external_ids:
            self._root_id_by_asset_external_id[asset_external_id] = found_by_external_id.get(asset_external_id)

    def resolve(self, node: NodeResponse) -> str:
        asset_external_id = as_external_id(get_view_property(node, self._view_id, self._property_id))
        if asset_external_id is None:
            raise TargetSpaceResolutionError(
                f"{node.as_id()} is missing {self._property_id}.", severity=Severity.failure
            )
        root_id = self._root_id_by_asset_external_id.get(asset_external_id)
        if root_id is None:
            raise TargetSpaceResolutionError(
                f"{node.as_id()}: asset {asset_external_id!r} referenced via {self._property_id} was not found.",
                severity=Severity.failure,
            )
        target_space = self._root_id_to_target_space.get(root_id)
        if target_space is None:
            raise TargetSpaceResolutionError(
                f"{node.as_id()}: asset {asset_external_id!r} root asset does not match a location sharing this "
                "source space.",
                severity=Severity.failure,
            )
        return target_space


def register_solution_tag_references(
    node: NodeResponse,
    target_space: str,
    instance_id_mapper: LocationSplitInstanceIdMapper,
) -> None:
    """Point this node's solutionTags at the copy of each tag in ``target_space``."""
    for properties in (node.properties or {}).values():
        if not isinstance(properties, dict):
            continue
        value = properties.get("solutionTags")
        if not isinstance(value, list):
            continue
        for item in value:
            if (external_id := as_external_id(item)) is not None:
                instance_id_mapper.register(external_id, target_space)
