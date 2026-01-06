from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId, ExternalIdMissing


class AssetRequest(RequestResource):
    external_id: str | None = None
    name: str
    parent_id: int | None = None
    parent_external_id: str | None = None
    description: str | None = None
    metadata: dict[str, str] | None = None
    data_set_id: int | None = None
    source: str | None = None
    labels: list[dict[Literal["externalId"], str]] | None = None
    geo_location: dict[str, JsonValue] | None = None

    def as_id(self) -> ExternalId | ExternalIdMissing:
        if self.external_id is not None:
            return ExternalId(external_id=self.external_id)
        else:
            return ExternalIdMissing()


class AssetAggregateItem(BaseModelObject):
    child_count: int | None = None
    depth: int | None = None
    path: list[dict[Literal["id"], int]] | None = None


class AssetResponse(ResponseResource[AssetRequest]):
    created_time: int
    last_updated_time: int
    root_id: int
    aggregates: AssetAggregateItem | None = None
    id: int
    external_id: str | None = None
    name: str
    parent_id: int | None = None
    parent_external_id: str | None = None
    description: str | None = None
    metadata: dict[str, str] | None = None
    data_set_id: int | None = None
    source: str | None = None
    labels: list[dict[Literal["externalId"], str]] | None = None
    geo_location: dict[str, JsonValue] | None = None

    def as_request_resource(self) -> AssetRequest:
        return AssetRequest.model_validate(self.dump())
