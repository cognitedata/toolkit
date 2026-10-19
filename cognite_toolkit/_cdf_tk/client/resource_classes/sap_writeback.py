from typing import Annotated, Any, Literal

from pydantic import Field, JsonValue, model_serializer, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, WritebackRequestId

WritebackRequestStatus = Literal["pending", "in_progress", "done", "failed"]

SAPEndpointType = Literal["notification", "attachment"]


class SAPInstance(BaseModelObject):
    """Configuration for an external SAP S/4HANA destination."""

    external_id: str
    gateway_url: str
    client: int
    username: str

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SAPInstanceRequest(SAPInstance, RequestResource):
    """Request resource for creating SAP instances.

    ``password`` is required to create an instance and is not returned by the API.
    """

    password: str


class SAPInstanceResponse(SAPInstance, ResponseResource[SAPInstanceRequest]):
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[SAPInstanceRequest]:
        return SAPInstanceRequest


class SAPEndpoint(BaseModelObject):
    """Configuration for an SAP S/4HANA OData endpoint used by writeback."""

    external_id: str
    endpoint_type: SAPEndpointType | str
    instance_id: str
    mapping_id: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SAPEndpointRequest(SAPEndpoint, RequestResource):
    """Request resource for creating SAP endpoints."""


class SAPEndpointResponse(SAPEndpoint, ResponseResource[SAPEndpointRequest]):
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[SAPEndpointRequest]:
        return SAPEndpointRequest


class SAPEndpointConnectionCheck(BaseModelObject):
    """Result of verifying connectivity to an SAP endpoint."""

    status: str
    detail: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _unwrap_error_envelope(cls, value: Any) -> Any:
        # The OpenAPI response wraps the check in ``error``, while the setup guide
        # returns ``status`` at the top level.
        if isinstance(value, dict) and "status" not in value and isinstance(value.get("error"), dict):
            return value["error"]
        return value

    @model_serializer(mode="wrap")
    def _wrap_error_envelope(self, handler: Any) -> Any:
        # Reverse of ``_unwrap_error_envelope``: nest the check back under ``error``
        # to restore the OpenAPI response envelope shape.
        return {"error": handler(self)}


class SchemaMapping(BaseModelObject):
    """In-flight transformation from CDF entities to SAP S/4HANA entities."""

    external_id: str
    expression: str

    @model_validator(mode="before")
    @classmethod
    def _lift_nested_expression(cls, value: Any) -> Any:
        # retrieve schema mappings is documented as returning the hosted-extractor
        # Mapping shape, with the expression nested under ``mapping``.
        if not isinstance(value, dict) or "expression" in value:
            return value
        mapping = value.get("mapping")
        if isinstance(mapping, dict) and "expression" in mapping:
            return {**value, "expression": mapping["expression"]}
        return value

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SchemaMappingRequest(SchemaMapping, RequestResource):
    """Request resource for creating schema mappings."""


class SchemaMappingResponse(SchemaMapping, ResponseResource[SchemaMappingRequest]):
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[SchemaMappingRequest]:
        return SchemaMappingRequest


class CDFClassicFileReference(BaseModelObject):
    type: Literal["cdf_classic_file"] = "cdf_classic_file"
    external_id: str


class CDMFileReference(BaseModelObject):
    type: Literal["cdm_file"] = "cdm_file"
    space: str
    external_id: str


SapFileReference = Annotated[CDFClassicFileReference | CDMFileReference, Field(discriminator="type")]


class WritebackRequestItem(BaseModelObject):
    key: str | None = None
    payload: dict[str, JsonValue]
    file_id: str | None = None
    file_reference: SapFileReference | None = None

    @model_validator(mode="after")
    def _one_file_reference(self) -> "WritebackRequestItem":
        if self.file_id is not None and self.file_reference is not None:
            raise ValueError("Only one of file_id or file_reference may be set")
        return self


class WritebackResponseItem(BaseModelObject):
    key: str | None = None  # Return on retrieve and list endpoints,
    payload: dict[str, JsonValue] | None = None  # Returned on retrieve and create endpoints
    sap_object_id: str | None = None  # Returned on retrieve and list endpoints


class WritebackRequestRequest(RequestResource):
    """Request resource for creating a writeback request."""

    endpoint_id: str
    request: list[WritebackRequestItem]

    def as_id(self) -> WritebackRequestId:
        raise ValueError("A writeback request ID is assigned when the request is created")


class WritebackRequestResponse(ResponseResource[WritebackRequestRequest]):
    request_id: str
    status: WritebackRequestStatus | str
    request: list[WritebackResponseItem]
    error_message: str | None = None
    created_time: int
    last_updated_time: int

    @classmethod
    def request_cls(cls) -> type[WritebackRequestRequest]:
        return WritebackRequestRequest

    def as_id(self) -> WritebackRequestId:
        return WritebackRequestId(request_id=self.request_id)
