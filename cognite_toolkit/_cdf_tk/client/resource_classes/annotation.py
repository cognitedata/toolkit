from typing import Any, Literal, TypeAlias

from pydantic import Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource

from .identifiers import InternalId

AnnotationStatus: TypeAlias = Literal["suggested", "rejected", "approved"]
AnnotationType: TypeAlias = Literal[
    "diagrams.AssetLink",
    "diagrams.FileLink",
    "diagrams.InstanceLink",
    "diagrams.Junction",
    "diagrams.Line",
    "diagrams.UnhandledSymbolObject",
    "diagrams.UnhandledTextObject",
    "documents.ExtractedText",
    "forms.Detection",
    "images.AssetLink",
    "images.Classification",
    "images.InstanceLink",
    "images.KeypointCollection",
    "images.ObjectDetection",
    "images.TextRegion",
    "isoplan.IsoPlanAnnotation",
    "pointcloud.BoundingVolume",
]


class Annotation(BaseModelObject):
    annotated_resource_type: str
    annotated_resource_id: int
    annotation_type: AnnotationType
    creating_app: str
    creating_app_version: str
    creating_user: str | None
    data: dict[str, JsonValue]
    status: AnnotationStatus


class AnnotationRequest(Annotation, UpdatableRequestResource):
    """Request data class for annotations."""

    # The 'id' field is not part of the request when creating a new resource,
    # but is needed when updating an existing resource.
    id: int | None = Field(default=None, exclude=True)

    def as_id(self) -> InternalId:
        if self.id is None:
            raise ValueError("Cannot convert AnnotationRequest to InternalId when id is None")
        return InternalId(id=self.id)

    def as_update(self, mode: Literal["patch", "replace"]) -> dict[str, Any]:
        """Converts the request to an update payload for the API."""
        if self.id is None:
            raise ValueError("id must be provided to create an update dictionary")
        return {
            "id": self.id,
            "update": {
                "annotationType": {"set": self.annotation_type},
                "data": {"set": self.data},
                "status": {"set": self.status},
            },
        }


class AnnotationResponse(Annotation, ResponseResource[AnnotationRequest]):
    """Response data class for annotations."""

    id: int
    created_time: int
    last_updated_time: int

    def as_id(self) -> InternalId:
        return InternalId(id=self.id)

    def as_request_resource(self) -> AnnotationRequest:
        return AnnotationRequest.model_validate(self.dump(), extra="ignore")
