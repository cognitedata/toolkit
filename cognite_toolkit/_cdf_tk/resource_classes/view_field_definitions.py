import re
import sys
from types import MappingProxyType
from typing import Any, ClassVar, Literal, cast

from pydantic import Field, ModelWrapValidatorHandler, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.constants import (
    CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN,
    DM_EXTERNAL_ID_PATTERN,
    DM_VERSION_PATTERN,
    SPACE_FORMAT_PATTERN,
)
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

from .base import BaseModelResource
from .container_field_definitions import ContainerReference

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self

KEY_PATTERN = re.compile(CONTAINER_AND_VIEW_PROPERTIES_IDENTIFIER_PATTERN)


class ViewReference(BaseModelResource):
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
    )
    identifier: str = Field(
        description="Identifier of the relation in the source view or container.",
        min_length=1,
        max_length=255,
    )


class ViewProperty(BaseModelResource):
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

    @model_validator(mode="wrap")
    @classmethod
    def find_property_type_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, ViewProperty):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid property type data '{type(data)}' expected dict")

        if cls is not ViewProperty:
            data_copy = dict(data)
            if cls in _CONNECTION_DEFINITION_CLASS_BY_TYPE.values():
                data_copy.pop("connectionType", None)
            return handler(data_copy)

        cls_: type[ContainerViewProperty] | type[ConnectionDefinition]
        if "container" in data:
            cls_ = ContainerViewProperty
        elif "connectionType" in data:
            connection_type = data.get("connectionType")
            if connection_type is None:
                raise ValueError("Missing 'connectionType' field in connection definition data")
            if connection_type not in _CONNECTION_DEFINITION_CLASS_BY_TYPE:
                raise ValueError(
                    f"invalid connection type '{connection_type}'. Expected one of {humanize_collection(_CONNECTION_DEFINITION_CLASS_BY_TYPE.keys(), bind_word='or')}"
                )
            cls_ = _CONNECTION_DEFINITION_CLASS_BY_TYPE[connection_type]
        else:
            raise ValueError(
                "Invalid Property data. If it is a connection definition, it must contain 'connectionType' field. If it is a view property, it must contain 'container' and 'containerPropertIdentifier' field."
            )

        data_copy = dict(data)
        data_copy.pop("connectionType", None)
        return cast(Self, cls_.model_validate(data_copy))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def serialize_property_type(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        if hasattr(self, "connection_type"):
            serialized_data["connectionType"] = self.__class__.connection_type
        return serialized_data


class ContainerViewProperty(ViewProperty):
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


class ConnectionDefinition(ViewProperty):
    connection_type: ClassVar[str]
    source: ViewReference = Field(
        description="Indicates the view which is either the target node(s) or the node(s) containing the direct relation property."
    )


class EdgeConnectionDefinition(ConnectionDefinition):
    connection_type: ClassVar[Literal["single_edge_connection", "multi_edge_connection"]]
    type: DirectRelationReference = Field(
        description="Reference to the node pointed to by the direct relation.",
    )
    edge_source: ViewReference | None = Field(
        default=None,
        description="Reference to the view from where this edge connection is inherited.",
    )
    direction: Literal["outwards", "inwards"]


class SingleEdgeConnectionDefinition(EdgeConnectionDefinition):
    connection_type = "single_edge_connection"


class MultiEdgeConnectionDefinition(EdgeConnectionDefinition):
    connection_type = "multi_edge_connection"


class ReverseDirectRelationConnectionDefinition(ConnectionDefinition):
    connection_type: ClassVar[Literal["single_reverse_direct_relation", "multi_reverse_direct_relation"]]
    through: ThroughRelationReference = Field(
        description="The view or container of the node containing the direct relation property.",
    )


class SingleReverseDirectRelationConnectionDefinition(ReverseDirectRelationConnectionDefinition):
    connection_type = "single_reverse_direct_relation"


class MultiReverseDirectRelationConnectionDefinition(ReverseDirectRelationConnectionDefinition):
    connection_type = "multi_reverse_direct_relation"


def get_connection_definition_type_leaf_classes(base_class: type[ConnectionDefinition]) -> list:
    subclasses = base_class.__subclasses__()
    result = []

    if not subclasses:
        if base_class is not ConnectionDefinition:
            result.append(base_class)
    else:
        for subclass in subclasses:
            result.extend(get_connection_definition_type_leaf_classes(subclass))

    return result


_CONNECTION_DEFINITION_CLASS_BY_TYPE: MappingProxyType[str, type[ConnectionDefinition]] = MappingProxyType(
    {
        c.connection_type: c
        for c in get_connection_definition_type_leaf_classes(ConnectionDefinition)
        if hasattr(c, "connection_type") and c.connection_type is not None
    }
)
