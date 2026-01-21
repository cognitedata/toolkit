from typing import Any, Literal

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    UpdatableRequestResource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId


class SourceRequestDefinition(UpdatableRequestResource):
    type: str
    external_id: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        output = super().as_update(mode)
        output["type"] = self.type
        return output


class SourceResponseDefinition(BaseModelObject):
    external_id: str
    created_time: int
    last_updated_time: int
