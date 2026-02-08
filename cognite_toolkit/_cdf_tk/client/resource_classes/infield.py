from typing import Any, ClassVar, Literal

from pydantic import JsonValue, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import DataModelReference
from cognite_toolkit._cdf_tk.utils.text import sanitize_instance_external_id

from .instance_api import (
    TypedInstanceIdentifier,
    TypedNodeIdentifier,
    TypedViewReference,
    WrappedInstanceListRequest,
    WrappedInstanceListResponse,
    WrappedInstanceRequest,
    WrappedInstanceResponse,
    move_properties,
)

INFIELD_LOCATION_CONFIG_VIEW_ID = TypedViewReference(
    space="cdf_infield", external_id="InFieldLocationConfig", version="v1"
)
INFIELD_ON_CDM_DATA_MODEL = DataModelReference(
    space="infield_cdm_source_desc_sche_asset_file_ts",
    external_id="InFieldOnCDM",
    version="v1",
)
INFIELD_CDM_LOCATION_CONFIG_VIEW_ID = TypedViewReference(
    space="infield_cdm_source_desc_sche_asset_file_ts", external_id="InFieldCDMLocationConfig", version="v1"
)
DATA_EXPLORATION_CONFIG_VIEW_ID = TypedViewReference(
    space="cdf_infield", external_id="DataExplorationConfig", version="v1"
)


class DataExplorationConfig(BaseModelObject):
    """Data Exploration Configuration resource class."""

    VIEW_ID: ClassVar[TypedViewReference] = DATA_EXPLORATION_CONFIG_VIEW_ID
    space: str | None = None
    external_id: str | None = None

    observations: dict[str, JsonValue] | None = None
    activities: dict[str, JsonValue] | None = None
    documents: dict[str, JsonValue] | None = None
    notifications: dict[str, JsonValue] | None = None
    assets: dict[str, JsonValue] | None = None

    @model_validator(mode="before")
    @classmethod
    def move_properties(cls, data: dict[str, Any]) -> dict[str, Any]:
        return move_properties(data, cls.VIEW_ID)


class InFieldLocationConfig(BaseModelObject):
    """Infield Location Configuration resource class.

    This class is used for both the response and request resource for Infield Location Configuration nodes.
    """

    VIEW_ID: ClassVar[TypedViewReference] = INFIELD_LOCATION_CONFIG_VIEW_ID
    root_location_external_id: str | None = None
    feature_toggles: dict[str, JsonValue] | None = None
    app_instance_space: str | None = None
    access_management: dict[str, JsonValue] | None = None
    data_filters: dict[str, JsonValue] | None = None
    data_exploration_config: DataExplorationConfig | None = None


class InFieldLocationConfigRequest(WrappedInstanceListRequest, InFieldLocationConfig):
    def dump_instances(self) -> list[dict[str, Any]]:
        space: str | None = None
        external_id: str | None = None
        if self.data_exploration_config:
            space = self.data_exploration_config.space or self.space
            if self.data_exploration_config.external_id:
                external_id = self.data_exploration_config.external_id
            else:
                candidate = f"{self.external_id}_data_exploration_config"
                external_id = sanitize_instance_external_id(candidate)

        properties = self.model_dump(
            by_alias=True,
            exclude_unset=True,
            exclude={"data_exploration_config", "instance_type", "space", "external_id"},
        )
        if space and external_id:
            properties["dataExplorationConfig"] = {"space": space, "externalId": external_id}
        output: list[dict[str, Any]] = [
            {
                "instanceType": self.instance_type,
                "space": self.space,
                "externalId": self.external_id,
                "sources": [
                    {
                        "source": self.VIEW_ID.dump(),
                        "properties": properties,
                    }
                ],
            }
        ]
        if space and external_id and self.data_exploration_config:
            output.append(
                {
                    "instanceType": "node",
                    "space": space,
                    "externalId": external_id,
                    "sources": [
                        {
                            "source": DataExplorationConfig.VIEW_ID.dump(),
                            "properties": self.data_exploration_config.model_dump(
                                by_alias=True, exclude_unset=True, exclude={"space", "external_id"}
                            ),
                        }
                    ],
                }
            )
        return output

    def as_ids(self) -> list[TypedInstanceIdentifier]:
        output: list[TypedInstanceIdentifier] = [self.as_id()]
        if (
            self.data_exploration_config
            and self.data_exploration_config.space
            and self.data_exploration_config.external_id
        ):
            output.append(
                TypedNodeIdentifier(
                    space=self.data_exploration_config.space,
                    external_id=self.data_exploration_config.external_id,
                )
            )
        return output


class InFieldLocationConfigResponse(WrappedInstanceListResponse, InFieldLocationConfig):
    def as_request_resource(self) -> InFieldLocationConfigRequest:
        return InFieldLocationConfigRequest.model_validate(self.dump(), extra="ignore")

    def as_ids(self) -> list[TypedInstanceIdentifier]:
        output: list[TypedInstanceIdentifier] = [TypedNodeIdentifier(space=self.space, external_id=self.external_id)]
        if (
            self.data_exploration_config
            and self.data_exploration_config.space
            and self.data_exploration_config.external_id
        ):
            output.append(
                TypedNodeIdentifier(
                    space=self.data_exploration_config.space,
                    external_id=self.data_exploration_config.external_id,
                )
            )
        return output


class InFieldCDMLocationConfig(BaseModelObject):
    name: str | None = None
    description: str | None = None
    feature_toggles: dict[str, JsonValue] | None = None
    access_management: dict[str, JsonValue] | None = None
    data_filters: dict[str, JsonValue] | None = None
    data_storage: dict[str, JsonValue] | None = None
    view_mappings: dict[str, JsonValue] | None = None
    disciplines: list[dict[str, JsonValue]] | None = None
    data_exploration_config: dict[str, JsonValue] | None = None


class InFieldCDMLocationConfigRequest(WrappedInstanceRequest, InFieldCDMLocationConfig):
    VIEW_ID: ClassVar[TypedViewReference] = INFIELD_CDM_LOCATION_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"

    def as_id(self) -> TypedNodeIdentifier:
        return TypedNodeIdentifier(space=self.space, external_id=self.external_id)


class InFieldCDMLocationConfigResponse(
    WrappedInstanceResponse[InFieldCDMLocationConfigRequest], InFieldCDMLocationConfig
):
    VIEW_ID: ClassVar[TypedViewReference] = INFIELD_CDM_LOCATION_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"

    def as_request_resource(self) -> InFieldCDMLocationConfigRequest:
        return InFieldCDMLocationConfigRequest.model_validate(self.dump(), extra="ignore")
