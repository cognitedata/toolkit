from typing import Any, Literal, TypeAlias

from pydantic import Field, JsonValue, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, ResponseResource, UpdatableRequestResource
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId

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


class BoundingBox(BaseModelObject):
    confidence: float | None = None
    x_min: float
    x_max: float
    y_min: float
    y_max: float


class AssetLinkData(BaseModelObject):
    asset_ref: InternalId | ExternalId
    text_region: BoundingBox
    symbol_region: BoundingBox | None = None
    page_number: int | None = None
    text: str | None = None
    description: str | None = None


class FileLinkData(BaseModelObject):
    file_ref: InternalId | ExternalId
    text_region: BoundingBox
    symbol_region: BoundingBox | None = None
    page_number: int | None = None
    text: str | None = None
    description: str | None = None


class AnnotationPoint(BaseModelObject):
    x: float
    y: float


class AnnotationPolygon(BaseModelObject):
    confidence: float | None = None
    vertices: list[AnnotationPoint]


class AnnotationPolyLine(BaseModelObject):
    confidence: float | None = None
    vertices: list[AnnotationPoint]


class AnnotationGeometry(BaseModelObject):
    bounding_box: BoundingBox | None = None
    polygon: AnnotationPolygon | None = None
    polyline: AnnotationPolyLine | None = None


class AnnotationInstanceRef(BaseModelObject):
    space: str
    external_id: str
    instance_type: Literal["node", "edge"]
    sources: list[dict[str, JsonValue]]


class ImageAssetLinkData(BaseModelObject):
    asset_ref: InternalId | ExternalId
    text: str
    text_region: BoundingBox
    object_region: AnnotationGeometry | None = None


class ImageInstanceLinkData(BaseModelObject):
    instance_ref: AnnotationInstanceRef
    text: str
    text_region: BoundingBox
    object_region: AnnotationGeometry | None = None
    confidence: float | None = None


AnnotationData: TypeAlias = (
    AssetLinkData | FileLinkData | ImageAssetLinkData | ImageInstanceLinkData | dict[str, JsonValue]
)

_ANNOTATION_DATA_CLS_BY_TYPE: dict[AnnotationType, type[BaseModelObject]] = {
    "diagrams.AssetLink": AssetLinkData,
    "diagrams.FileLink": FileLinkData,
    "images.AssetLink": ImageAssetLinkData,
    "images.InstanceLink": ImageInstanceLinkData,
}


class Annotation(BaseModelObject):
    annotated_resource_type: str
    annotated_resource_id: int
    annotation_type: AnnotationType
    creating_app: str
    creating_app_version: str
    creating_user: str | None
    data: AnnotationData
    status: AnnotationStatus

    @model_validator(mode="before")
    @classmethod
    def parse_data_by_annotation_type(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values

        annotation_type = values.get("annotationType", values.get("annotation_type"))
        data = values.get("data")
        if not isinstance(annotation_type, str) or not isinstance(data, dict):
            return values

        data_cls = _ANNOTATION_DATA_CLS_BY_TYPE.get(annotation_type)  # type: ignore[arg-type]
        if data_cls is None:
            return values

        parsed = dict(values)
        parsed["data"] = data_cls.model_validate(data)
        return parsed


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
                "data": {
                    "set": self.data.dump()
                    if isinstance(self.data, AssetLinkData | FileLinkData | ImageAssetLinkData | ImageInstanceLinkData)
                    else self.data
                },
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

    @classmethod
    def request_cls(cls) -> type[AnnotationRequest]:
        return AnnotationRequest
