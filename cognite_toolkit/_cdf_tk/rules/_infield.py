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
)
from cognite_toolkit._cdf_tk.yaml_classes.view_field_definitions import ViewReference

_REQUIRED_PROPERTIES: dict[str, frozenset[str]] = {
    "assetActivitiesCardView": frozenset(
        {"sourceId", "name", "status", "type", "mainAsset", "scheduledEndTime", "scheduledStartTime"}
    ),
    "assetNotificationsCardView": frozenset(
        {"name", "sourceId", "type", "status", "description", "asset", "createdDate", "priority"}
    ),
}

# (resource, card_key, view_id, required properties)
_CardViewRef = tuple[BuiltResource, str, ViewId, frozenset[str]]
# (resource, config_key, view_id, configured field keys)
_FieldConfigRef = tuple[BuiltResource, str, ViewId, frozenset[str]]
_DEFAULT_ASSET_VIEW_ID = ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")


class InFieldCDMViewPropertiesRuleSet(ToolkitGlobalRuleSet):
    CODE_PREFIX = "INFIELD-CDM"
    DISPLAY_NAME = "InField CDM view properties"

    def get_status(self) -> RuleSetStatus:
        if not self.client:
            return RuleSetStatus(
                code="reduced",
                message=(
                    "InField CDM view property validation requires a client. "
                    "Provide client credentials to validate card views and field configs against CDF."
                ),
            )
        return RuleSetStatus(
            code="ready",
            message="Will validate InField CDM card views and field configs against CDF view properties.",
        )

    def validate(self) -> Iterable[ConsistencyError]:
        if self.client is None:
            return
        config_type = ResourceType(
            resource_folder=InFieldCDMLocationConfigIO.folder_name,
            kind=InFieldCDMLocationConfigIO.kind,
        )
        card_view_refs: list[_CardViewRef] = []
        field_config_refs: list[_FieldConfigRef] = []

        for module in self.modules:
            for resource in module.resources:
                if not resource.can_verify:
                    continue
                if resource.type != config_type:
                    continue
                card_refs, field_refs = self._collect_refs(resource)
                card_view_refs.extend(card_refs)
                field_config_refs.extend(field_refs)

        if not card_view_refs and not field_config_refs:
            return

        unique_view_ids = list({ref[2] for ref in card_view_refs} | {ref[2] for ref in field_config_refs})
        retrieved = self.client.tool.views.retrieve(unique_view_ids, include_inherited_properties=True)
        views_by_id: dict[ViewId, ViewResponse] = {v.as_id(): v for v in retrieved}

        for resource, card_key, view_id, required in card_view_refs:
            yield from self._check_required_properties(resource, card_key, view_id, required, views_by_id)

        for resource, config_key, view_id, field_keys in field_config_refs:
            yield from self._check_field_config_keys(resource, config_key, view_id, field_keys, views_by_id)

    @staticmethod
    def _asset_view_id_for_card_config(config: InFieldCDMLocationConfigYAML) -> ViewId:
        if config.view_mappings and config.view_mappings.asset is not None:
            return config.view_mappings.asset.as_id()
        return _DEFAULT_ASSET_VIEW_ID

    @staticmethod
    def _collect_refs(
        resource: BuiltResource,
    ) -> tuple[list[_CardViewRef], list[_FieldConfigRef]]:
        raw_data = read_yaml_file(resource.build_path, expected_output="dict")
        config = InFieldCDMLocationConfigYAML.model_validate(raw_data)

        card_refs: list[_CardViewRef] = []
        field_refs: list[_FieldConfigRef] = []

        if config.data_exploration_config:
            for attr, card_key in INFIELD_CDM_CARD_VIEW_ATTR_TO_JSON_KEY.items():
                mapping: ViewReference | None = getattr(config.data_exploration_config, attr, None)
                if mapping is None:
                    continue
                view_id = mapping.as_id()
                required = _REQUIRED_PROPERTIES[card_key]
                card_refs.append((resource, card_key, view_id, required))

            if card_config := config.data_exploration_config.asset_properties_card_config:
                field_refs.append(
                    (
                        resource,
                        "assetPropertiesCardConfig",
                        InFieldCDMViewPropertiesRuleSet._asset_view_id_for_card_config(config),
                        frozenset(card_config.keys()),
                    )
                )

        if config.view_mappings and config.view_mappings.observation:
            for observation_config in config.view_mappings.observation:
                if not observation_config.fields_config:
                    continue
                field_refs.append(
                    (
                        resource,
                        "observation.fieldsConfig",
                        observation_config.view.as_id(),
                        frozenset(observation_config.fields_config.keys()),
                    )
                )

        return card_refs, field_refs

    def _check_required_properties(
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

    def _check_field_config_keys(
        self,
        resource: BuiltResource,
        config_key: str,
        view_id: ViewId,
        field_keys: frozenset[str],
        views_by_id: dict[ViewId, ViewResponse],
    ) -> Iterable[ConsistencyError]:
        view = views_by_id.get(view_id)
        if view is None:
            return

        unknown = field_keys - set(view.properties.keys())
        if unknown:
            yield ConsistencyError(
                code=f"{self.CODE_PREFIX}-UNKNOWN-VIEW-PROPERTY",
                message=(
                    f"View {view_id!s} used for {config_key!r} in {resource.source_path.name!r} "
                    f"does not have properties: {humanize_collection(sorted(unknown))}."
                ),
                fix=f"Use property names that exist on the view: {humanize_collection(sorted(unknown))}.",
            )
