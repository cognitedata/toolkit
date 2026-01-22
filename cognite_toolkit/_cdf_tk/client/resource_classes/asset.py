from typing import ClassVar, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource

from .identifiers import ExternalId


class Asset(BaseModelObject):
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

    def as_id(self) -> ExternalId:
        if self.external_id is None:
            raise ValueError("Cannot convert AssetRequest to ExternalId when external_id is None")
        return ExternalId(external_id=self.external_id)


class AssetRequest(Asset, UpdatableRequestResource):
    container_fields: ClassVar[frozenset[str]] = frozenset({"metadata", "labels"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset({"parent_id", "parent_external_id"})


class AssetAggregateItem(BaseModelObject):
    child_count: int
    depth: int
    path: list[dict[Literal["id"], int]]


class AssetResponse(Asset, ResponseResource[AssetRequest]):
    created_time: int
    last_updated_time: int
    root_id: int
    aggregates: AssetAggregateItem | None = None
    id: int

    def as_request_resource(self) -> AssetRequest:
        return AssetRequest.model_validate(self.dump(), extra="ignore")
