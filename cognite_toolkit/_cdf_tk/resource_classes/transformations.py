from typing import Literal

from cognite.client.data_classes import TransformationWrite
from pydantic import Field, model_serializer
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler

from .base import BaseModelResource, ToolkitResource
from .transformation_destination import Destination


class AuthenticationClientIdSecret(BaseModelResource):
    client_id: str = Field(description="Client Id.")
    client_secret: str = Field(description="Client Secret.")


class OIDCCredential(AuthenticationClientIdSecret):
    scopes: str | list[str] | None = Field(default=None, description="Scopes for the authentication.")
    token_uri: str = Field(description="OAuth token url.")
    cdf_project_name: str = Field(description="CDF project name.")
    audience: str | None = Field(default=None, description="Audience for the authentication.")


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
