from collections.abc import Mapping, Sequence
from pathlib import Path

from pydantic import JsonValue, TypeAdapter
from pydantic.config import ExtraValues

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import InFieldCDMLocationConfigResponse
from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.exceptions import ToolkitMigrationError
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content, safe_read

# Maps the ViewToViewMapping.source_view.external_id of each APM_SourceData mapping to the "type" key
# used both in InFieldCDMLocationConfig.dataFilters and .viewMappings.
SOURCE_DATA_TYPE_BY_VIEW_EXTERNAL_ID: dict[str, str] = {
    "APM_Activity": "maintenanceOrder",
    "APM_Operation": "operation",
    "APM_Notification": "notification",
}
# The dataFilters key for each APM_SourceData type.
_DATA_FILTER_KEY_BY_TYPE: dict[str, str] = {
    "maintenanceOrder": "maintenanceOrders",
    "operation": "operations",
    "notification": "notifications",
}
# The viewMappings key(s) for each APM_SourceData type. maintenanceOrder has two possible aliases:
# 'maintenanceOrder' and the legacy 'activity'.
_VIEW_MAPPING_KEYS_BY_TYPE: dict[str, tuple[str, ...]] = {
    "maintenanceOrder": ("maintenanceOrder", "activity"),
    "operation": ("operation",),
    "notification": ("notification",),
}

# Identifies the canonical AppConfig node when a project has more than one (in practice, projects have
# exactly one AppConfig node).
APP_CONFIG_V2_EXTERNAL_ID = "APP_CONFIG_V2"
DEFAULT_SOURCE_DATA_SPACE = "APM_SourceData"
DEFAULT_SOURCE_DATA_VERSION = "1"
# Maps the AppConfig entity key (featureConfiguration.viewMappings.<entity>) to the default APM_SourceData
# view external ID for that entity.
SOURCE_VIEW_EXTERNAL_ID_BY_ENTITY: dict[str, str] = {
    "activity": "APM_Activity",
    "operation": "APM_Operation",
    "notification": "APM_Notification",
}
ENTITY_BY_SOURCE_VIEW_EXTERNAL_ID: dict[str, str] = {
    external_id: entity for entity, external_id in SOURCE_VIEW_EXTERNAL_ID_BY_ENTITY.items()
}


def create_apm_source_data_mappings(extra: ExtraValues | None = None) -> list[ViewToViewMapping]:
    mappings_path = Path(__file__).parent / "apm_source_data_mappings.yaml"

    content = safe_read(mappings_path)
    mappings_dict = read_yaml_content(content)
    if not isinstance(mappings_dict, list):
        raise ValueError(f"Expected a list of mappings in the YAML file, but got {type(mappings_dict).__name__}")
    return TypeAdapter(list[ViewToViewMapping]).validate_python(mappings_dict, extra=extra)


def get_first_instance_space(data_filters: Mapping[str, JsonValue] | None, type_key: str) -> str | None:
    """Extracts ``dataFilters.<type_key>.instanceSpaces[0]`` from a raw InFieldCDMLocationConfig.dataFilters dict.

    Only the first instance space is ever used by Infield at runtime for these resource filters, the rest
    of the array (if any) is ignored.
    """
    if not isinstance(data_filters, dict):
        return None
    filter_ = data_filters.get(_DATA_FILTER_KEY_BY_TYPE[type_key])
    if not isinstance(filter_, dict):
        return None
    instance_spaces = filter_.get("instanceSpaces")
    if not isinstance(instance_spaces, list) or not instance_spaces:
        return None
    return instance_spaces[0] if isinstance(instance_spaces[0], str) else None


def resolve_source_data_view_ids(
    configs: Sequence[InFieldCDMLocationConfigResponse], target_space: str
) -> dict[str, ViewId]:
    """Determine which custom maintenanceOrder/operation/notification views, if any, APM_SourceData
    should be migrated to for a given target instance space, based on the ``viewMappings`` entries of the
    InField CDM location configs already deployed for that space.

    Args:
        configs: All deployed InField CDM location configs (``client.infield.cdm_config.list()``).
        target_space: The instance space APM_SourceData is being migrated to.

    Returns:
        Dict of type key ("maintenanceOrder", "operation", "notification") to the custom ``ViewId`` to
        migrate that type's data to. A type is omitted if no location targeting ``target_space`` has a
        custom view configured for it (in which case the default cdf_idm view should be used).

    Raises:
        ToolkitMigrationError: If locations targeting ``target_space`` specify conflicting views for the
            same type.

    """
    resolved: dict[str, ViewId] = {}
    for type_key, data_filter_key in _DATA_FILTER_KEY_BY_TYPE.items():
        view_id_by_location: dict[str, ViewId] = {}
        for config in configs:
            if get_first_instance_space(config.data_filters, type_key) != target_space:
                continue
            if not isinstance(config.view_mappings, dict):
                continue
            view: object | None = None
            for key in _VIEW_MAPPING_KEYS_BY_TYPE[type_key]:
                candidate = config.view_mappings.get(key)
                if isinstance(candidate, dict):
                    view = candidate
                    break
            if not isinstance(view, dict):
                continue
            view_id_by_location[config.external_id] = ViewId(
                space=str(view.get("space")),
                external_id=str(view.get("externalId")),
                version=str(view.get("version")),
            )

        distinct_view_ids = set(view_id_by_location.values())
        if not distinct_view_ids:
            continue
        if len(distinct_view_ids) == 1:
            resolved[type_key] = next(iter(distinct_view_ids))
            continue

        conflicts = ", ".join(f"{location}={view_id!s}" for location, view_id in view_id_by_location.items())
        raise ToolkitMigrationError(
            f"You have configured multiple InField locations targeting the same {data_filter_key} instance "
            f"space {target_space!r} with different {type_key} views: {conflicts}. Therefore, Toolkit cannot "
            f"automatically determine which view to migrate {type_key} data to. You need to ensure all "
            f"locations targeting this instance space share the same {type_key} view. Distinct views found: "
            f"{humanize_collection([str(view_id) for view_id in distinct_view_ids])}."
        )
    return resolved


def select_primary_apm_config(configs: Sequence[APMConfigResponse]) -> APMConfigResponse | None:
    """AppConfig's ``viewMappings`` (and space/version overrides) are project-global settings, not
    per-location. If a project has an ``APP_CONFIG_V2`` node, that is the canonical config to read them
    from; otherwise fall back to the first config (in practice, projects have exactly one AppConfig node).
    """
    return next((config for config in configs if config.external_id == APP_CONFIG_V2_EXTERNAL_ID), None) or (
        configs[0] if configs else None
    )


def resolve_apm_source_data_view_ids(configs: Sequence[APMConfigResponse]) -> dict[str, ViewId]:
    """Determine the APM_SourceData view (space/externalId/version) to migrate each entity ("activity",
    "operation", "notification") from, based on the project's AppConfig node.

    Resolution priority per entity:
        1. ``featureConfiguration.viewMappings.<entity>`` on the AppConfig node, if set.
        2. ``customerDataSpaceId`` (+ ``customerDataSpaceVersion``) on the AppConfig node, if set.
        3. The hardcoded default: ``APM_SourceData/APM_<Entity>/1``.

    Args:
        configs: All deployed APM configs (``client.infield.apm_config.list()``).

    Returns:
        Dict of entity key ("activity", "operation", "notification") to the ``ViewId`` to migrate that
        entity's data from.

    """
    config = select_primary_apm_config(configs)
    feature_configuration = config.feature_configuration if config else None
    view_mappings = feature_configuration.view_mappings if feature_configuration else None
    customer_data_space_id = config.customer_data_space_id if config else None
    customer_data_space_version = config.customer_data_space_version if config else None

    resolved: dict[str, ViewId] = {}
    for entity, default_external_id in SOURCE_VIEW_EXTERNAL_ID_BY_ENTITY.items():
        mapping = view_mappings.get(entity) if isinstance(view_mappings, dict) else None
        space = mapping.get("space") if isinstance(mapping, dict) else None
        external_id = mapping.get("externalId") if isinstance(mapping, dict) else None
        version = mapping.get("version") if isinstance(mapping, dict) else None
        if isinstance(space, str) and isinstance(external_id, str) and isinstance(version, str):
            resolved[entity] = ViewId(space=space, external_id=external_id, version=version)
        elif isinstance(customer_data_space_id, str):
            resolved[entity] = ViewId(
                space=customer_data_space_id,
                external_id=default_external_id,
                version=customer_data_space_version or DEFAULT_SOURCE_DATA_VERSION,
            )
        else:
            resolved[entity] = ViewId(
                space=DEFAULT_SOURCE_DATA_SPACE, external_id=default_external_id, version=DEFAULT_SOURCE_DATA_VERSION
            )
    return resolved


def resolve_apm_source_data_instance_spaces(configs: Sequence[APMConfigResponse]) -> set[str]:
    """Collect the candidate APM_SourceData instance spaces to migrate from, across all root locations
    configured on the given APM configs.

    Note: AppConfig also has a top-level ``rootLocationsConfiguration.locations`` field, but that is only
    thin metadata for populating a location picker dropdown in the Infield UI (``value``/``label`` pairs).
    Infield itself always reads the rich per-location config, including ``sourceDataInstanceSpace``, from
    ``featureConfiguration.rootLocationConfigurations``, so we do the same here.

    For each root location, in priority order:
        1. ``featureConfiguration.rootLocationConfigurations[n].sourceDataInstanceSpace``.
        2. ``customerDataSpaceId`` (top-level fallback), used when a location has no source data instance
           space of its own, or when a config has no root locations configured at all.

    Args:
        configs: All deployed APM configs (``client.infield.apm_config.list()``).

    Returns:
        The set of distinct candidate source instance spaces found across all configs/locations.

    """
    spaces: set[str] = set()
    for config in configs:
        customer_data_space_id = config.customer_data_space_id
        root_location_configurations = (
            config.feature_configuration.root_location_configurations if config.feature_configuration else None
        )
        if not root_location_configurations:
            if isinstance(customer_data_space_id, str):
                spaces.add(customer_data_space_id)
            continue

        for location in root_location_configurations:
            if location.source_data_instance_space:
                spaces.add(location.source_data_instance_space)
            elif isinstance(customer_data_space_id, str):
                spaces.add(customer_data_space_id)
    return spaces
