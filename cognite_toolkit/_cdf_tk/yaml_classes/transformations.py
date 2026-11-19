from typing import Any, Literal

from pydantic import Field, field_validator

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .authentication import AuthenticationClientIdSecret, OIDCCredential
from .base import ToolkitResource
from .transformation_destination import DestinationType


class TransformationYAML(ToolkitResource):
    external_id: str = Field(description="The external ID provided by the client.")
    name: str = Field(description="Name of the transformation.")
    ignore_null_fields: bool = Field(
        description="Indicates how null values are handled on updates: ignore or set null."
    )
    destination: DestinationType | None = Field(default=None, description="Destination data type.")
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
    data_domain_external_id: str | None = Field(
        default=None,
        description="External ID of the data domain the transformation belongs to. Defaults to UNGOVERNED.",
        min_length=1,
        max_length=100,
        pattern=r"^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
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

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)

    @field_validator("authentication", mode="before")
    @classmethod
    def validate_serialization(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        if "read" in value or "write" in value:
            return {k: cls._validate_auth_value(v) if isinstance(v, dict) else v for k, v in value.items()}
        return cls._validate_auth_value(value)

    @classmethod
    def _validate_auth_value(cls, value: dict[str, Any]) -> AuthenticationClientIdSecret | OIDCCredential:
        if "scopes" in value or "tokenUri" in value or "cdfProjectName" in value or "audience" in value:
            return OIDCCredential.model_validate(value)
        return AuthenticationClientIdSecret.model_validate(value)
