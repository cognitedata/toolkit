from typing import Any, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    ResponseResource,
    RequestResource
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client._types import Metadata



class ChartFolderRequest(RequestResource):
    folder_external_id: str
    folder_name: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.folder_external_id)

class AlertRules(BaseModelObject):
    deduplication: JsonValue | None = None
    excludeSender: JsonValue | None = None


class ChartFolderResponse(ResponseResource):
    id: int
    parent_id: int
    external_id: str
    parent_external_id: str
    name: str | None = None
    description: str | None = None
    metadata: Metadata | None = None
    alert_rules: AlertRules | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
