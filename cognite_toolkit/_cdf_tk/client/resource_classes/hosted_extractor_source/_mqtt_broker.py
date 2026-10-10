import builtins
from typing import Any, ClassVar, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client._types import Metadata

from ._base import SourceRequestDefinition, SourceResponseDefinition


class MQTTBrokerAuthentication(BaseModelObject):
    """Basic auth for a CDF-hosted MQTT broker.

    ``password`` is returned only when the source is created or the password is reset.
    """

    type: Literal["basic"] = "basic"
    username: str
    password: str | None = None


class MQTTBrokerSource(BaseModelObject):
    type: Literal["mqtt_broker"] = "mqtt_broker"
    name: str | None = None
    description: str | None = None
    metadata: Metadata | None = None


class MQTTBrokerSourceRequest(MQTTBrokerSource, SourceRequestDefinition):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata"})
    # Update-only. Create rejects this field, and the update API expects ``password.reset``.
    reset_password: bool = Field(default=False, exclude=True)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        output = super().as_update(mode)
        if self.reset_password:
            output.setdefault("update", {})["password"] = {"reset": True}
        return output


class MQTTBrokerSourceResponse(
    SourceResponseDefinition,
    MQTTBrokerSource,
    ResponseResource[MQTTBrokerSourceRequest],
):
    host: str
    port: int
    authentication: MQTTBrokerAuthentication

    @classmethod
    def request_cls(cls) -> builtins.type[MQTTBrokerSourceRequest]:
        return MQTTBrokerSourceRequest
