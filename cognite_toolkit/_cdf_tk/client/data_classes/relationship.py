from typing import Literal

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


class LabelRef(BaseModelObject):
    """Reference to a label."""

    external_id: str


class RelationshipBase(BaseModelObject):
    """Base class for relationship with common fields."""

    external_id: str
    source_external_id: str
    source_type: Literal["asset", "timeSeries", "file", "event", "sequence"]
    target_external_id: str
    target_type: Literal["asset", "timeSeries", "file", "event", "sequence"]
    start_time: int | None = None
    end_time: int | None = None
    confidence: float | None = None
    data_set_id: int | None = None
    labels: list[LabelRef] | None = None


class RelationshipRequest(RelationshipBase, RequestResource):
    """Request resource for creating/updating relationships."""

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class RelationshipResponse(RelationshipBase, ResponseResource[RelationshipRequest]):
    """Response resource for relationships."""

    created_time: int
    last_updated_time: int

    def as_request_resource(self) -> RelationshipRequest:
        return RelationshipRequest.model_validate(self.dump(), extra="ignore")
