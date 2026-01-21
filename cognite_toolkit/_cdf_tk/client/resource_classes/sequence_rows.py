from typing import Literal

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId


class SequenceColumn(BaseModelObject):
    """Represents a column in a sequence."""

    external_id: str
    name: str | None = None
    description: str | None = None
    value_type: Literal["STRING", "DOUBLE", "LONG"] | None = None
    metadata: dict[str, str] | None = None


class SequenceRow(BaseModelObject):
    """Represents a row in a sequence."""

    row_number: int
    values: list[str | int | float]


class SequenceRows(BaseModelObject):
    """Base class for sequence rows with common fields."""

    rows: list[SequenceRow]


class SequenceRowsRequest(SequenceRows, RequestResource):
    """Request resource for inserting sequence rows."""

    external_id: str
    columns: list[str]

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SequenceRowsResponse(SequenceRows, ResponseResource[SequenceRowsRequest]):
    """Response resource for sequence rows.

    Note: The CDF API for sequence rows typically doesn't return the standard
    created_time/last_updated_time fields in the same way as other resources.
    The response contains the sequence identification and the row data.
    """

    external_id: str | None = None
    id: int
    columns: list[SequenceColumn]

    def as_request_resource(self) -> SequenceRowsRequest:
        dumped = self.dump()
        dumped["columns"] = [col.external_id for col in self.columns]
        return SequenceRowsRequest.model_validate(dumped, extra="ignore")
