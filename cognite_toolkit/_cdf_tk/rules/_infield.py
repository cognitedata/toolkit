from collections.abc import Iterable
from typing import ClassVar

import yaml
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import ResourceType
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, FailedValidation
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


class InFieldCDMViewPropertiesRuleSet(ToolkitGlobalRuleSet):
    CODE: ClassVar[str] = "INFIELD-CDM-VIEW-PROPERTIES"
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

    def validate(self) -> Iterable[ConsistencyError | FailedValidation]:
        config_type = ResourceType(
            resource_folder=InFieldCDMLocationConfigIO.folder_name,
            kind=InFieldCDMLocationConfigIO.kind,
        )
        for module in self.modules:
            for resource in module.resources:
                if not resource.can_verify:
                    continue
                if resource.type != config_type:
                    continue
                try:
                    yield from self._validate_config(resource)
                except (ValidationError, ValueError, yaml.YAMLError, ToolkitAPIError, OSError) as e:
                    yield FailedValidation(
                        message=(f"InField CDM view property validation failed for {resource.build_path.name!r}: {e}"),
                        source=str(resource.identifier),
                    )

    def _validate_config(self, resource: BuiltResource) -> Iterable[ConsistencyError]:
        if self.client is None:
            return
        client = self.client

        raw_data = read_yaml_file(resource.build_path, expected_output="dict")
        config = InFieldCDMLocationConfigYAML.model_validate(raw_data)
        if not config.data_exploration_config:
            return

        for attr, card_key in INFIELD_CDM_CARD_VIEW_ATTR_TO_JSON_KEY.items():
            mapping: ViewMapping | None = getattr(config.data_exploration_config, attr, None)
            if mapping is None:
                continue
            view_id = ViewId(space=mapping.space, external_id=mapping.external_id, version=mapping.version)
            required = _REQUIRED_PROPERTIES[card_key]
            yield from self._validate_view_properties(resource, view_id, card_key, required, client)

    def _validate_view_properties(
        self,
        resource: BuiltResource,
        view_id: ViewId,
        card_key: str,
        required: frozenset[str],
        client: ToolkitClient,
    ) -> Iterable[ConsistencyError]:
        views = client.tool.views.retrieve([view_id], include_inherited_properties=True)
        if not views:
            yield ConsistencyError(
                code=self.CODE,
                message=(
                    f"View {view_id!s} referenced as {card_key!r} in "
                    f"{resource.source_path.name!r} was not found in CDF."
                ),
                fix="Ensure the view exists in CDF or update the card view mapping.",
            )
            return

        view = views[0]
        missing = required - set(view.properties.keys())
        if missing:
            yield ConsistencyError(
                code=self.CODE,
                message=(
                    f"View {view_id!s} used as {card_key!r} in {resource.source_path.name!r} "
                    f"is missing required properties: {humanize_collection(sorted(missing))}."
                ),
                fix=f"Ensure the view has these properties: {humanize_collection(sorted(missing))}.",
            )
