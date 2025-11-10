import sys
from collections import UserList
from typing import Any, ClassVar, Literal

from cognite.client import CogniteClient
from pydantic import JsonValue, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.protocols import ResourceRequestListProtocol, ResourceResponseListProtocol
from cognite_toolkit._cdf_tk.utils.text import sanitize_instance_external_id

from .base import ResponseResource
from .instance_api import InstanceRequestResource, ViewReference

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

INFIELD_LOCATION_CONFIG_VIEW_ID = ViewReference(space="cdf_infield", external_id="InFieldLocationConfig", version="v1")
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
    access_management: dict[str, JsonValue] | None = None
    data_filters: dict[str, JsonValue] | None = None
    data_exploration_config: DataExplorationConfig | None = None

    def as_request_resource(self) -> "InfieldLocationConfig":
        return self

    def as_write(self) -> Self:
        return self

    @field_validator("data_exploration_config", mode="before")
    @classmethod
    def generate_identifier_if_missing(cls, value: Any, info: ValidationInfo) -> Any:
        """We do not require the user to specify the space and externalId for the data exploration config."""
        if isinstance(value, dict):
            if value.get("space") is None:
                value["space"] = info.data["space"]
            if value.get("externalId") is None:
                external_id = info.data["external_id"]
                candidate = f"{external_id}_data_exploration_config"
                value["externalId"] = sanitize_instance_external_id(candidate)
        return value


class InfieldLocationConfigList(
    UserList[InfieldLocationConfig],
    ResourceResponseListProtocol,
    ResourceRequestListProtocol,
):
    """A list of InfieldLocationConfig objects."""

    _RESOURCE = InfieldLocationConfig
    data: list[InfieldLocationConfig]

    def __init__(self, initlist: list[InfieldLocationConfig] | None = None, **_: Any) -> None:
        super().__init__(initlist or [])

    def dump(self, camel_case: bool = True) -> list[dict[str, Any]]:
        """Serialize the list of InfieldLocationConfig objects to a list of dictionaries."""
        return [item.dump(camel_case) for item in self.data]

    @classmethod
    def load(
        cls, data: list[dict[str, Any]], cognite_client: CogniteClient | None = None
    ) -> "InfieldLocationConfigList":
        """Deserialize a list of dictionaries to an InfieldLocationConfigList."""
        items = [InfieldLocationConfig.model_validate(item) for item in data]
        return cls(items)

    def as_write(self) -> Self:
        return self
