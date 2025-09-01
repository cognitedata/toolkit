from typing import Any, Literal

from cognite.client.data_classes import TransformationWrite
from pydantic import Field, field_validator, model_serializer
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from .authentication import AuthenticationClientIdSecret, OIDCCredential
from .base import ToolkitResource
from .transformation_destination import Destination


class TransformationYAML(ToolkitResource):
    _cdf_resource = TransformationWrite
    external_id: str = Field(description="The external ID provided by the client.")
    name: str = Field(description="Name of the transformation.")
    ignore_null_fields: bool = Field(
        description="Indicates how null values are handled on updates: ignore or set null."
    )
    destination: Destination | None = Field(default=None, description="Destination data type.")
    query: str | None = Field(default=None, description="SQL query of the transformation.")
    conflict_mode: Literal["abort", "delete", "update", "upsert"] | None = Field(
        default=None,
        description="Behavior when the data already exists.",
    )
    is_public: bool | None = Field(
        default=None,
        description="Indicates if the transformation is visible to all in project or only to the owner.",
    )
    authentication: (
        AuthenticationClientIdSecret
        | OIDCCredential
        | dict[Literal["read", "write"], AuthenticationClientIdSecret | OIDCCredential]
        | None
    ) = Field(
        default=None,
        description="Authentication information for the transformation.",
    )
    data_set_external_id: str | None = Field(
        default=None,
        description="External ID of the data set to which the transformation belongs.",
    )
    tags: list[str] | None = Field(
        default=None,
        description="List of tags for the Transformation.",
        max_length=5,
    )
    queryFile: str | None = Field(
        default=None,
        description="Used by Toolkit: Path to the SQL file containing the query for the transformation.",
    )

    @model_serializer(mode="wrap")
    def serialize_transformation(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        serialized_data = handler(self)
        if self.destination:
            serialized_data["destination"] = self.destination.model_dump(**vars(info))
        return serialized_data

    @field_validator("authentication", mode="before")
    @classmethod
    def validate_serialization(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "read" in value or "write" in value:
            return {
                k: AuthenticationClientIdSecret.model_validate(v) if isinstance(v, dict) else v
                for k, v in value.items()
            }
        if "scopes" in value or "tokenUri" in value or "cdfProjectName" in value or "audience" in value:
            return OIDCCredential.model_validate(value)
        return AuthenticationClientIdSecret.model_validate(value)
