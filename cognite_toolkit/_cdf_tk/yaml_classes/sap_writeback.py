from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, WritebackRequestId

from .base import BaseModelResource, ToolkitResource


class SAPInstanceYAML(ToolkitResource):
    """SAP S/4HANA destination used by the writeback service."""

    external_id: str = Field(
        description="External ID that uniquely identifies the SAP instance.",
        min_length=1,
    )
    gateway_url: str = Field(
        description="URL of the SAP gateway.",
        min_length=1,
    )
    client: int = Field(description="SAP client number.")
    username: str = Field(description="SAP user name.", min_length=1)
    password: str = Field(
        description="SAP password. Required to create an instance and not returned by the API.",
        min_length=1,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SchemaMappingYAML(ToolkitResource):
    """In-flight transformation from CDF entities to SAP S/4HANA entities."""

    external_id: str = Field(
        description="External ID that uniquely identifies the schema mapping.",
        min_length=1,
    )
    expression: str = Field(
        description="Mapping expression from CDF fields to SAP fields.",
        min_length=1,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SAPEndpointYAML(ToolkitResource):
    """SAP S/4HANA OData endpoint used by writeback."""

    external_id: str = Field(
        description="External ID that uniquely identifies the SAP endpoint.",
        min_length=1,
    )
    endpoint_type: str = Field(
        description="Endpoint type. Known values are 'notification' and 'attachment'.",
        min_length=1,
    )
    instance_id: str = Field(
        description="External ID of the SAP instance this endpoint connects to.",
        min_length=1,
    )
    mapping_id: str | None = Field(
        default=None,
        description="External ID of the schema mapping applied to payloads sent to this endpoint.",
        min_length=1,
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class CDFClassicFileReferenceYAML(BaseModelResource):
    """Reference to a classic CDF file."""

    type: Literal["cdf_classic_file"] = "cdf_classic_file"
    external_id: str = Field(description="External ID of the classic file.", min_length=1)


class CDMFileReferenceYAML(BaseModelResource):
    """Reference to a data modeling file."""

    type: Literal["cdm_file"] = "cdm_file"
    space: str = Field(description="Space that contains the file.", min_length=1)
    external_id: str = Field(description="External ID of the file.", min_length=1)


SapFileReferenceYAML = Annotated[CDFClassicFileReferenceYAML | CDMFileReferenceYAML, Field(discriminator="type")]


class WritebackRequestItemYAML(BaseModelResource):
    """One item in a writeback request."""

    key: str | None = Field(default=None, description="Optional caller-defined key for this item.")
    payload: dict[str, Any] = Field(description="SAP payload for this item.")
    file_id: str | None = Field(default=None, description="External ID of a classic CDF file to attach.")
    file_reference: SapFileReferenceYAML | None = Field(
        default=None,
        description="File to attach. Use either fileId or fileReference.",
    )

    @model_validator(mode="after")
    def _one_file_reference(self) -> "WritebackRequestItemYAML":
        if self.file_id is not None and self.file_reference is not None:
            raise ValueError("Only one of fileId or fileReference may be set")
        return self


class WritebackRequestYAML(ToolkitResource):
    """Request to write CDF data back to an SAP endpoint.

    ``requestId`` identifies the request in Toolkit. CDF assigns its own request ID on create,
    so this value is not sent to the API.
    """

    request_id: str = Field(
        description="Toolkit identifier for the writeback request. CDF assigns the stored request ID on create.",
        min_length=1,
    )
    endpoint_id: str = Field(
        description="External ID of the SAP endpoint that receives the request.",
        min_length=1,
    )
    request: list[WritebackRequestItemYAML] = Field(
        description="Items to write back to SAP.",
        min_length=1,
    )

    def as_id(self) -> WritebackRequestId:
        return WritebackRequestId(request_id=self.request_id)
