from abc import ABC
from typing import Any

from pydantic import field_serializer
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from ._references import DataModelReference, ViewReference
from ._view import ViewResponse


class DataModel(BaseModelObject, ABC):
    """Cognite Data Model resource.

    Data models group and structure views into reusable collections.
    A data model contains a set of views where the node types can
    refer to each other with direct relations and edges.
    """

    space: str
    external_id: str
    version: str
    description: str | None = None

    def as_id(self) -> DataModelReference:
        return DataModelReference(
            space=self.space,
            external_id=self.external_id,
            version=self.version,
        )


class DataModelRequest(DataModel, RequestResource):
    views: list[ViewReference] | None = None

    @field_serializer("views", mode="plain")
    @classmethod
    def serialize_views(
        cls, views: list[ViewReference] | None, info: FieldSerializationInfo
    ) -> list[dict[str, Any]] | None:
        if views is None:
            return None
        return [{**view.model_dump(**vars(info)), "type": "view"} for view in views]


class DataModelResponse(DataModel, ResponseResource[DataModelRequest]):
    views: list[ViewReference] | None = None
    created_time: int
    last_updated_time: int
    is_global: bool

    def as_request_resource(self) -> DataModelRequest:
        return DataModelRequest.model_validate(self.model_dump(by_alias=True), extra="ignore")

    @field_serializer("views", mode="plain")
    @classmethod
    def serialize_views(
        cls, views: list[ViewReference] | None, info: FieldSerializationInfo
    ) -> list[dict[str, Any]] | None:
        if views is None:
            return None
        return [{**view.model_dump(**vars(info)), "type": "view"} for view in views]


class DataModelResponseWithViews(DataModel, ResponseResource[DataModelRequest]):
    views: list[ViewResponse] | None = None
    created_time: int
    last_updated_time: int
    is_global: bool

    def as_request_resource(self) -> DataModelRequest:
        return DataModelRequest.model_validate(self.model_dump(by_alias=True), extra="ignore")
