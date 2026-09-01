import re
from typing import Any

from pydantic import Field, field_validator, model_serializer
from pydantic_core.core_schema import SerializationInfo, SerializerFunctionWrapHandler, ValidationInfo

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewId as ViewReferenceId
from cognite_toolkit._cdf_tk.constants import (
    CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN,
    DM_EXTERNAL_ID_PATTERN,
    DM_VERSION_PATTERN,
    FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS,
    FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER,
    SPACE_FORMAT_PATTERN,
)
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import ToolkitResource
from .view_field_definitions import ContainerViewProperty, ViewProperty, ViewReference

KEY_PATTERN = re.compile(CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN)


class ViewYAML(ToolkitResource):
    space: str = Field(
        description="Id of the space that the view belongs to.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    stream_id: list[str] | None = Field(
        default=None,
        description="If set, this is a record view and list the stream ids that the view is associated with.",
    )
    external_id: str = Field(
        description="External-id of the view.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )
    version: str = Field(
        description="Version of the view.",
        max_length=43,
        pattern=DM_VERSION_PATTERN,
    )
    name: str | None = Field(
        default=None,
        description="name for the view.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="Description of the view.",
        max_length=1024,
    )
    filter: dict[str, Any] | None = Field(
        default=None,
        description="A filter Domain Specific Language (DSL) used to create advanced filter queries.",
    )
    implements: list[ViewReference] | None = Field(
        default=None,
        description="References to the views from where this view will inherit properties.",
    )
    properties: dict[str, ViewProperty] | None = Field(
        default=None, description="Set of properties to apply to the View."
    )

    def as_id(self) -> ViewReferenceId:
        return ViewReferenceId(space=self.space, external_id=self.external_id, version=self.version)

    @field_validator("external_id")
    @classmethod
    def check_forbidden_external_id_value(cls, val: str) -> str:
        """Check the external_id not present in forbidden set"""
        if val in FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS:
            raise ValueError(
                f"'{val}' is a reserved view External ID. Reserved External IDs are: {humanize_collection(FORBIDDEN_CONTAINER_AND_VIEW_EXTERNAL_IDS)}"
            )
        return val

    @field_validator("properties")
    @classmethod
    def validate_properties_identifier(cls, val: dict[str, str]) -> dict[str, str]:
        """Validate properties Identifier"""
        for key in val.keys():
            if not KEY_PATTERN.match(key):
                raise ValueError(f"Property '{key}' does not match the required pattern: {KEY_PATTERN.pattern}")
            if key in FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER:
                raise ValueError(
                    f"'{key}' is a reserved property identifier. Reserved identifiers are: {humanize_collection(FORBIDDEN_CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER)}"
                )
        return val

    @field_validator("properties", mode="after")
    @classmethod
    def record_views_only_support_container_properties(
        cls, val: dict[str, ViewProperty] | None, info: ValidationInfo
    ) -> dict[str, ViewProperty] | None:
        """Validate that record views only support container properties."""
        if info.data["stream_id"] is not None and val is not None:
            invalid_properties = [key for key, prop in val.items() if not isinstance(prop, ContainerViewProperty)]
            if invalid_properties:
                raise ValueError(
                    f"Record views only support container properties. The following properties are invalid for record views: {humanize_collection(invalid_properties)}"
                )
        return val

    @model_serializer(mode="wrap")
    def serialize_container(self, handler: SerializerFunctionWrapHandler, info: SerializationInfo) -> dict:
        serialized_data = handler(self)
        if self.properties:
            serialized_data["properties"] = {
                key: value.model_dump(**vars(info)) for key, value in self.properties.items()
            }
        return serialized_data
