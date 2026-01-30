import sys
from typing import Any, ClassVar, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from .instance_api import (
    TypedNodeIdentifier,
    ViewReference,
    WrappedInstanceListRequest,
    WrappedInstanceListResponse,
    WrappedInstanceRequest,
    WrappedInstanceResponse,
)

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

INFIELD_LOCATION_CONFIG_VIEW_ID = ViewReference(space="cdf_infield", external_id="InFieldLocationConfig", version="v1")
INFIELD_CDM_LOCATION_CONFIG_VIEW_ID = ViewReference(
    space="infield_cdm_source_desc_sche_asset_file_ts", external_id="InFieldCDMLocationConfig", version="v1"
)
DATA_EXPLORATION_CONFIG_VIEW_ID = ViewReference(space="cdf_infield", external_id="DataExplorationConfig", version="v1")


class DataExplorationConfig(BaseModelObject):
    """Data Exploration Configuration resource class."""

    VIEW_ID: ClassVar[ViewReference] = DATA_EXPLORATION_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"
    space: str | None = None
    external_id: str | None = None

    observations: dict[str, JsonValue] | None = None
    activities: dict[str, JsonValue] | None = None
    documents: dict[str, JsonValue] | None = None
    notifications: dict[str, JsonValue] | None = None
    assets: dict[str, JsonValue] | None = None


class InFieldLocationConfig(BaseModelObject):
    """Infield Location Configuration resource class.

    This class is used for both the response and request resource for Infield Location Configuration nodes.
    """

    VIEW_ID: ClassVar[ViewReference] = INFIELD_LOCATION_CONFIG_VIEW_ID
    root_location_external_id: str | None = None
    feature_toggles: dict[str, JsonValue] | None = None
    app_instance_space: str | None = None
    access_management: dict[str, JsonValue] | None = None
    data_filters: dict[str, JsonValue] | None = None
    data_exploration_config: DataExplorationConfig | None = None


class InFieldLocationConfigRequest(WrappedInstanceListRequest, InFieldLocationConfig):
    def dump_instances(self) -> list[dict[str, Any]]:
        raise NotImplementedError()


class InFieldLocationConfigResponse(WrappedInstanceListResponse, InFieldLocationConfig):
    def as_request_resource(self) -> InFieldLocationConfigRequest:
        return InFieldLocationConfigRequest.model_validate(self.dump(), extra="ignore")

    @classmethod
    def load_query_response(cls) -> Self:
        raise NotImplementedError()


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
    VIEW_ID: ClassVar[ViewReference] = INFIELD_CDM_LOCATION_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"

    def as_id(self) -> TypedNodeIdentifier:
        return TypedNodeIdentifier(space=self.space, external_id=self.external_id)


class InFieldCDMLocationConfigResponse(
    WrappedInstanceResponse[InFieldCDMLocationConfigRequest], InFieldCDMLocationConfig
):
    VIEW_ID: ClassVar[ViewReference] = INFIELD_CDM_LOCATION_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"

    def as_request_resource(self) -> InFieldCDMLocationConfigRequest:
        return InFieldCDMLocationConfigRequest.model_validate(self.dump(), extra="ignore")
