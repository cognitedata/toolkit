import sys
from collections import UserList
from typing import Any, ClassVar, Literal

from cognite.client import CogniteClient
from pydantic import JsonValue

from cognite_toolkit._cdf_tk.protocols import (
    ResourceRequestListProtocol,
    ResourceResponseListProtocol,
)

from .base import BaseResourceList, ResponseResource
from .instance_api import InstanceRequestResource, ViewReference

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

INFIELD_LOCATION_CONFIG_VIEW_ID = ViewReference(space="cdf_infield", external_id="InFieldLocationConfig", version="v1")
INFIELD_CDM_LOCATION_CONFIG_VIEW_ID = ViewReference(
    space="infield_cdm_source_desc_sche_asset_file_ts", external_id="InFieldCDMLocationConfig", version="v1"
)
DATA_EXPLORATION_CONFIG_VIEW_ID = ViewReference(space="cdf_infield", external_id="DataExplorationConfig", version="v1")


class DataExplorationConfig(InstanceRequestResource):
    """Data Exploration Configuration resource class."""

    VIEW_ID: ClassVar[ViewReference] = DATA_EXPLORATION_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"

    observations: dict[str, JsonValue] | None = None
    activities: dict[str, JsonValue] | None = None
    documents: dict[str, JsonValue] | None = None
    notifications: dict[str, JsonValue] | None = None
    assets: dict[str, JsonValue] | None = None


class InfieldLocationConfig(
    ResponseResource["InfieldLocationConfig"],
    InstanceRequestResource,
):
    """Infield Location Configuration resource class.

    This class is used for both the response and request resource for Infield Location Configuration nodes.
    """

    VIEW_ID: ClassVar[ViewReference] = INFIELD_LOCATION_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"

    root_location_external_id: str | None = None
    feature_toggles: dict[str, JsonValue] | None = None
    app_instance_space: str | None = None

    def as_request_resource(self) -> "InfieldLocationConfig":
        return self

    def as_write(self) -> Self:
        return self


class InfieldLocationConfigList(
    BaseResourceList[InfieldLocationConfig],
    ResourceResponseListProtocol,
    ResourceRequestListProtocol,
):
    """A list of InfieldLocationConfig objects."""

    _RESOURCE = InfieldLocationConfig

    def as_write(self) -> Self:
        return self


class InFieldCDMLocationConfig(
    ResponseResource["InFieldCDMLocationConfig"],
    InstanceRequestResource,
):
    """InField CDM Location Configuration resource class.

    This class is used for both the response and request resource for InField CDM Location Configuration nodes.
    """

    VIEW_ID: ClassVar[ViewReference] = INFIELD_CDM_LOCATION_CONFIG_VIEW_ID
    instance_type: Literal["node"] = "node"

    name: str | None = None
    description: str | None = None
    feature_toggles: dict[str, JsonValue] | None = None
    access_management: dict[str, JsonValue] | None = None
    data_filters: dict[str, JsonValue] | None = None
    data_storage: dict[str, JsonValue] | None = None
    view_mappings: dict[str, JsonValue] | None = None
    disciplines: list[dict[str, JsonValue]] | None = None
    data_exploration_config: dict[str, JsonValue] | None = None

    def as_request_resource(self) -> "InFieldCDMLocationConfig":
        return self

    def as_write(self) -> Self:
        return self


class InFieldCDMLocationConfigList(
    UserList[InFieldCDMLocationConfig],
    ResourceResponseListProtocol,
    ResourceRequestListProtocol,
):
    """A list of InFieldCDMLocationConfig objects."""

    _RESOURCE = InFieldCDMLocationConfig
    data: list[InFieldCDMLocationConfig]

    def __init__(self, initlist: list[InFieldCDMLocationConfig] | None = None, **_: Any) -> None:
        super().__init__(initlist or [])

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        """Serialize the list of InFieldCDMLocationConfig objects to a list of dictionaries."""
        return [item.dump(camel_case) for item in self.data]

    @classmethod
    def load(
        cls, data: list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> "InFieldCDMLocationConfigList":
        """Deserialize a list of dictionaries to an InFieldCDMLocationConfigList."""
        items = [InFieldCDMLocationConfig.model_validate(item) for item in data]
        return cls(items)

    def as_write(self) -> Self:
        return self
