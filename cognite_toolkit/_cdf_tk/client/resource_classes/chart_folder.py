from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId


class ChartFolderRequest(RequestResource):
    folder_external_id: str = Field(alias="folderExternalID")
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

    @classmethod
    def request_cls(cls) -> type[ChartFolderRequest]:
        return ChartFolderRequest

    def as_request_resource(self) -> ChartFolderRequest:
        return ChartFolderRequest(
            # The server prefixes external IDs with "charts-folder-" when returning them,
            # so we need to remove that prefix to get the original external ID for the request.
            folder_external_id=self.external_id.removeprefix("charts-folder-"),
            folder_name=self.name,
        )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id.removeprefix("charts-folder-"))
