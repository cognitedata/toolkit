from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._view import ViewResponse
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError
from cognite_toolkit._cdf_tk.resource_ios import InFieldCDMLocationConfigIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitGlobalRuleSet
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import read_yaml_file
from cognite_toolkit._cdf_tk.yaml_classes import InFieldCDMLocationConfigYAML
from cognite_toolkit._cdf_tk.yaml_classes.infield_cdm_location_config import (
    INFIELD_CDM_CARD_VIEW_ATTR_TO_JSON_KEY,
    ViewMapping,
)

_REQUIRED_PROPERTIES: dict[str, frozenset[str]] = {
    "assetActivitiesCard": frozenset({"sourceId", "name", "status", "type", "mainAsset"}),
    "assetNotificationsCard": frozenset(
        {"sourceId", "type", "status", "description", "asset", "createdDate", "priority"}
    ),
}

# (resource, card_key, view_id, required properties)
_CardViewRef = tuple[BuiltResource, str, ViewId, frozenset[str]]


class InFieldCDMViewPropertiesRuleSet(ToolkitGlobalRuleSet):
    CODE_PREFIX = "INFIELD-CDM"
    DISPLAY_NAME = "InField CDM view properties"

    def get_status(self) -> RuleSetStatus:
        if not self.client:
            return RuleSetStatus(
                code="reduced",
                message=(
                    "InField CDM view property validation requires a client. "
                    "Provide client credentials to validate required properties on card views."
                ),
            )
        return RuleSetStatus(
            code="ready",
            message="Will validate required properties on InField CDM card views against CDF.",
        )

    def validate(self) -> Iterable[ConsistencyError]:
        if self.client is None:
            return
        config_type = ResourceType(
            resource_folder=InFieldCDMLocationConfigIO.folder_name,
            kind=InFieldCDMLocationConfigIO.kind,
        )
        card_view_refs: list[_CardViewRef] = []

        for module in self.modules:
            for resource in module.resources:
                if not resource.can_verify:
                    continue
                if resource.type != config_type:
                    continue
                card_view_refs.extend(self._collect_card_view_refs(resource))

        if not card_view_refs:
            return

        unique_view_ids = list({ref[2] for ref in card_view_refs})
        retrieved = self.client.tool.views.retrieve(unique_view_ids, include_inherited_properties=True)
        views_by_id: dict[ViewId, ViewResponse] = {v.as_id(): v for v in retrieved}

        for resource, card_key, view_id, required in card_view_refs:
            yield from self._check_view(resource, card_key, view_id, required, views_by_id)

    @staticmethod
    def _collect_card_view_refs(resource: BuiltResource) -> list[_CardViewRef]:
        raw_data = read_yaml_file(resource.build_path, expected_output="dict")
        config = InFieldCDMLocationConfigYAML.model_validate(raw_data)
        if not config.data_exploration_config:
            return []

        refs: list[_CardViewRef] = []
        for attr, card_key in INFIELD_CDM_CARD_VIEW_ATTR_TO_JSON_KEY.items():
            mapping: ViewMapping | None = getattr(config.data_exploration_config, attr, None)
            if mapping is None:
                continue
            view_id = ViewId(space=mapping.space, external_id=mapping.external_id, version=mapping.version)
            required = _REQUIRED_PROPERTIES[card_key]
            refs.append((resource, card_key, view_id, required))
        return refs

    def _check_view(
        self,
        resource: BuiltResource,
        card_key: str,
        view_id: ViewId,
        required: frozenset[str],
        views_by_id: dict[ViewId, ViewResponse],
    ) -> Iterable[ConsistencyError]:
        view = views_by_id.get(view_id)
        if view is None:
            return

        missing = required - set(view.properties.keys())
        if missing:
            yield ConsistencyError(
                code=f"{self.CODE_PREFIX}-VIEW-MISSING-PROPERTIES",
                message=(
                    f"View {view_id!s} used as {card_key!r} in {resource.source_path.name!r} "
                    f"is missing required properties: {humanize_collection(sorted(missing))}."
                ),
                fix=f"Ensure the view has these properties: {humanize_collection(sorted(missing))}.",
            )
