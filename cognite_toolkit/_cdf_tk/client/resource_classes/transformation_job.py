from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import Destination


class TransformationJobMetricResponse(BaseModelObject):
    timestamp: int
    name: str
    count: int


class TransformationJobResponse(BaseModelObject):
    id: int
    uuid: str
    status: Literal["Running", "Created", "Completed", "Failed"] | str
    transformation_id: int
    transformation_external_id: str
    source_project: str
    destination_project: str
    destination: Destination
    conflict_mode: Literal["abort", "delete", "update", "upsert"] | str
    query: str
    ignore_null_fields: bool
    created_time: int
    error: str | None = None
    started_time: int | None = None
    finished_time: int | None = None
    last_seen_time: int | None = None

    def as_id(self) -> InternalId:
        return InternalId(id=self.id)
