import re
from abc import ABC
from typing import Annotated, Any, Literal

from pydantic import Field
from pydantic.functional_validators import BeforeValidator

from cognite_toolkit._cdf_tk.client import identifiers
from cognite_toolkit._cdf_tk.constants import (
    CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN,
    DM_EXTERNAL_ID_PATTERN,
    DM_VERSION_PATTERN,
    SPACE_FORMAT_PATTERN,
)

from .base import BaseModelResource
from .container_field_definitions import ContainerReference

KEY_PATTERN = re.compile(CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN)


class ViewReference(BaseModelResource, populate_by_name=True):
    type: Literal["view"] = "view"
    space: str = Field(
        description="Id of the space that the view belongs to.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
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

    def as_id(self) -> identifiers.ViewId:
        """Converts the reference to a ViewId identifier."""
        return identifiers.ViewId(space=self.space, external_id=self.external_id, version=self.version)


class DirectRelationReference(BaseModelResource):
    space: str = Field(
        description="Id of the space that the instance belongs to.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the instance.",
        min_length=1,
        max_length=255,
    )


class ThroughRelationReference(BaseModelResource):
    source: ViewReference | ContainerReference = Field(
        description="Reference to the view or container from where this relation is inherited.",
        discriminator="type",
    )
    identifier: str = Field(
        description="Identifier of the relation in the source view or container.",
        min_length=1,
        max_length=255,
    )


class ViewProperty(BaseModelResource, ABC):
    """Base for view properties.

    Mapped container properties have no ``connectionType`` in YAML. A before-validator
    fills in ``primary_property`` so the same discriminator can select them.
    """

    connection_type: str
    name: str | None = Field(
        default=None,
        description="Name of the property.",
        max_length=255,
    )
    description: str | None = Field(
        default=None,
        description="Description of the content and suggested use for this property..",
        max_length=1024,
    )


class ContainerViewProperty(ViewProperty):
    connection_type: Literal["primary_property"] = Field(default="primary_property", exclude=True)
    container: ContainerReference = Field(
        description="Reference to the container where this property is defined.",
    )
    container_property_identifier: str = Field(
        description="Identifier of the property in the container.",
        min_length=1,
        max_length=255,
        pattern=CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN,
    )
    source: ViewReference | None = Field(
        default=None,
        description="Indicates on what type a referenced direct relation is expected to be. Only applicable for direct relation properties.",
    )


class ConnectionDefinition(ViewProperty, ABC):
    source: ViewReference = Field(
        description="Indicates the view which is either the target node(s) or the node(s) containing the direct relation property."
    )


class EdgeConnectionDefinition(ConnectionDefinition, ABC):
    type: DirectRelationReference = Field(
        description="Reference to the node pointed to by the direct relation.",
    )
    edge_source: ViewReference | None = Field(
        default=None,
        description="Reference to the view from where this edge connection is inherited.",
    )
    direction: Literal["outwards", "inwards"]


class SingleEdgeConnectionDefinition(EdgeConnectionDefinition):
    connection_type: Literal["single_edge_connection"] = "single_edge_connection"


class MultiEdgeConnectionDefinition(EdgeConnectionDefinition):
    connection_type: Literal["multi_edge_connection"] = "multi_edge_connection"


class ReverseDirectRelationConnectionDefinition(ConnectionDefinition, ABC):
    through: ThroughRelationReference = Field(
        description="The view or container of the node containing the direct relation property.",
    )


class SingleReverseDirectRelationConnectionDefinition(ReverseDirectRelationConnectionDefinition):
    connection_type: Literal["single_reverse_direct_relation"] = "single_reverse_direct_relation"


class MultiReverseDirectRelationConnectionDefinition(ReverseDirectRelationConnectionDefinition):
    connection_type: Literal["multi_reverse_direct_relation"] = "multi_reverse_direct_relation"


def _ensure_view_property_connection_type(value: Any) -> Any:
    """Mapped properties omit ``connectionType``; treat that as ``primary_property``."""
    if isinstance(value, ViewProperty):
        return value
    if isinstance(value, dict) and "connectionType" not in value and "connection_type" not in value:
        return {**value, "connectionType": "primary_property"}
    return value


ViewPropertyType = Annotated[
    ContainerViewProperty
    | SingleEdgeConnectionDefinition
    | MultiEdgeConnectionDefinition
    | SingleReverseDirectRelationConnectionDefinition
    | MultiReverseDirectRelationConnectionDefinition,
    Field(discriminator="connection_type"),
    BeforeValidator(_ensure_view_property_connection_type),
]
