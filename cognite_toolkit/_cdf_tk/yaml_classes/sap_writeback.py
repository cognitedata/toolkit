from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource


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
