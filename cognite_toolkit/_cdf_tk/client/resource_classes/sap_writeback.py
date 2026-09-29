from typing import Any, Literal

from pydantic import model_validator

from cognite_toolkit._cdf_tk.client._resource_base import (
    BaseModelObject,
    RequestResource,
    ResponseResource,
)
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

SAPEndpointType = Literal["notification", "attachment"]


class SAPInstance(BaseModelObject):
    """Configuration for an external SAP S/4HANA destination."""

    external_id: str
    gateway_url: str
    client: int
    username: str | None = None

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)


class SAPInstanceRequest(SAPInstance, RequestResource):
    """Request resource for creating SAP instances.

    ``password`` is required to create an instance and is not returned by the API.
    """

    username: str
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
    endpoint_type: SAPEndpointType
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
    error_message: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _unwrap_error_envelope(cls, value: Any) -> Any:
        # The OpenAPI response wraps the check in ``error``, while the setup guide
        # returns ``status`` at the top level.
        if isinstance(value, dict) and "status" not in value and isinstance(value.get("error"), dict):
            return value["error"]
        return value


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
