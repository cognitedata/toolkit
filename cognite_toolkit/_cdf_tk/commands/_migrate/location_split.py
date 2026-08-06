"""Helpers for splitting shared legacy Infield instance spaces into per-location CDM spaces."""

from collections import defaultdict
from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import APMConfigResponse
from cognite_toolkit._cdf_tk.utils.text import fix_invalid_space_name

CDM_SPACE_SUFFIX = "_cdm"
CFG_SPACE_SUFFIX = "_cfg"


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
