from typing import ClassVar, Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestUpdateable, ResponseResource

from .identifiers import InternalId

AnnotationStatus = Literal["suggested", "rejected", "approved"]


class Annotation(BaseModelObject):
    annotated_resource_type: str
    annotated_resource_id: int
    annotation_type: str
    creating_app: str
    creating_app_version: str
    creating_user: str | None
    data: dict[str, JsonValue]
    status: AnnotationStatus


class AnnotationRequest(Annotation, RequestUpdateable):
    container_fields: ClassVar[frozenset[str]] = frozenset({"data"})
    non_nullable_fields: ClassVar[frozenset[str]] = frozenset()

    def as_id(self) -> InternalId:
        raise ValueError("AnnotationRequest does not have an id. Use AnnotationResponse.as_id() instead.")


class AnnotationResponse(Annotation, ResponseResource[AnnotationRequest]):
    id: int
    created_time: int
    last_updated_time: int

    def as_id(self) -> InternalId:
        return InternalId(id=self.id)

    def as_request_resource(self) -> AnnotationRequest:
        return AnnotationRequest.model_validate(self.dump(), extra="ignore")
