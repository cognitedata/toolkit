from typing import Literal

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject, RequestResource, ResponseResource

from .identifiers import ExternalId, InternalId, InternalOrExternalId


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
    values: list[JsonValue]


class SequenceRowsBase(BaseModelObject):
    """Base class for sequence rows with common fields."""

    external_id: str | None = None
    id: int | None = None
    columns: list[str]
    rows: list[SequenceRow]


class SequenceRowsRequest(SequenceRowsBase, RequestResource):
    """Request resource for inserting sequence rows."""

    def as_id(self) -> InternalOrExternalId:
        if self.external_id is not None:
            return ExternalId(external_id=self.external_id)
        if self.id is not None:
            return InternalId(id=self.id)
        raise ValueError("Either external_id or id must be set")


class SequenceRowsResponse(SequenceRowsBase, ResponseResource[SequenceRowsRequest]):
    """Response resource for sequence rows.

    Note: The CDF API for sequence rows typically doesn't return the standard
    created_time/last_updated_time fields in the same way as other resources.
    The response contains the sequence identification and the row data.
    """

    id: int

    def as_request_resource(self) -> SequenceRowsRequest:
        return SequenceRowsRequest.model_validate(self.dump(), extra="ignore")
