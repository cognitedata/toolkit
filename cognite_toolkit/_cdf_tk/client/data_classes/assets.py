from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.utils.http_client import BaseModelObject, RequestResource

from .base import ResponseResource


class AssetRequest(RequestResource):
    name: str
    external_id: str | None = None
    parent_id: int | None = None
    parent_external_id: str | None = None
    description: str | None = None
    metadata: dict[str, str] | None = None
    data_set_id: int | None = None
    source: str | None = None
    labels: list[str | dict[Literal["externalId"], str]] | None = None
    geo_location: dict[str, JsonValue] | None = None

    def as_id(self) -> str | None:
        return self.external_id


class Aggregates(BaseModelObject):
    child_count: int
    depth: int
    path: list[int]


class AssetResponse(ResponseResource[AssetRequest]):
    id: int
    name: str
    external_id: str | None = None
    parent_id: int | None = None
    parent_external_id: str | None = None
    description: str | None = None
    metadata: dict[str, str] | None = None
    data_set_id: int | None = None
    source: str | None = None
    labels: list[str | dict[Literal["externalId"], str]] | None = None
    geo_location: dict[str, JsonValue] | None = None
    created_time: int
    last_updated_time: int
    root_id: int
    aggregates: Aggregates | None = None

    def as_id(self) -> int:
        return self.id

    def as_request_resource(self) -> AssetRequest:
        return AssetRequest._load(self.dump())
