from datetime import date, datetime, time, timedelta
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import AliasGenerator, BaseModel, BeforeValidator, Field, PlainSerializer
from pydantic.alias_generators import to_camel

from ._entities import ParsedEntity, parse_entities, parse_entity

CellValueType: TypeAlias = str | int | float | bool | datetime | date | time | timedelta | None


def title_case(value: str) -> str:
    """Convert a string to title case, treating underscores and hyphens as word separators."""
    return " ".join(word.capitalize() for word in value.replace("_", " ").replace("-", " ").split())


def parse_entity_str(value: Any) -> ParsedEntity | None:
    if value is None:
        return None
    if isinstance(value, ParsedEntity):
        return value
    if not isinstance(value, str):
        raise ValueError("Expected string or ParsedEntity")
    try:
        return parse_entity(value)
    except ValueError as error:
        raise ValueError(f"Invalid entity syntax: {error}") from error


def parse_entities_str(value: Any) -> list[ParsedEntity] | None:
    if value is None:
        return None
    if isinstance(value, list) and all(isinstance(item, ParsedEntity) for item in value):
        return value
    if not isinstance(value, str):
        raise ValueError("Expected string or list of ParsedEntity")
    try:
        return parse_entities(value)
    except ValueError as error:
        raise ValueError(f"Invalid entity list syntax: {error}") from error


Entity = Annotated[ParsedEntity, BeforeValidator(parse_entity_str), PlainSerializer(func=str)]
EntityList = Annotated[
    list[ParsedEntity],
    BeforeValidator(parse_entities_str),
    PlainSerializer(func=lambda values: ",".join(str(item) for item in values)),
]


class TableObj(
    BaseModel,
    extra="ignore",
    alias_generator=AliasGenerator(
        alias=to_camel,
        validation_alias=title_case,
        serialization_alias=title_case,
    ),
    populate_by_name=True,
): ...


class MetadataValue(TableObj):
    key: str
    value: CellValueType


class DMSProperty(TableObj):
    view: Entity
    view_property: str
    name: str | None = None
    description: str | None = None
    connection: Entity | None = None
    value_type: Entity
    min_count: int | None = None
    max_count: int | None = None
    immutable: bool | None = None
    default: CellValueType | None = None
    auto_increment: bool | None = None
    container: Entity | None = None
    container_property: str | None = None
    container_property_name: str | None = None
    container_property_description: str | None = None
    index: EntityList | None = None
    constraint: EntityList | None = None


class EntityTableFilter(BaseModel):
    """A hasData or nodeType filter written in a Views table cell."""

    type: Literal["hasData", "nodeType"]
    entities: EntityList

    def __str__(self) -> str:
        entities_str = ",".join(str(entity) for entity in self.entities)
        return f"{self.type}({entities_str})"


class RAWFilterTableFilter(BaseModel):
    """A view filter that holds a raw JSON filter."""

    type: Literal["rawFilter"] = "rawFilter"
    filter: str

    def __str__(self) -> str:
        return f"rawFilter({self.filter})"


def _parse_table_filter(value: Any) -> dict[str, str] | EntityTableFilter | RAWFilterTableFilter | None:
    if value is None:
        return None
    if isinstance(value, EntityTableFilter | RAWFilterTableFilter):
        return value
    if not isinstance(value, str):
        return value
    filter_configs = {
        "hasdata(": ("hasData", "entities"),
        "nodetype(": ("nodeType", "entities"),
        "rawfilter(": ("rawFilter", "filter"),
    }
    lowered = value.casefold()
    for prefix, (filter_type, field_name) in filter_configs.items():
        if lowered.startswith(prefix) and lowered.endswith(")"):
            return {"type": filter_type, field_name: value[len(prefix) : -1]}
    return {"type": "rawFilter", "filter": value}


TableViewFilter = Annotated[
    EntityTableFilter | RAWFilterTableFilter,
    Field(discriminator="type"),
    BeforeValidator(_parse_table_filter),
    PlainSerializer(func=str),
]


class DMSView(TableObj):
    view: Entity
    name: str | None = None
    description: str | None = None
    implements: EntityList | None = None
    filter: TableViewFilter | None = None


class DMSContainer(TableObj):
    container: Entity
    name: str | None = None
    description: str | None = None
    constraint: EntityList | None = None
    used_for: str | None = None


class DMSEnum(TableObj):
    collection: str
    value: str
    name: str | None = None
    description: str | None = None


class DMSNode(TableObj):
    node: Entity


def normalize_table_names(data: Any) -> Any:
    if not isinstance(data, dict):
        return data
    return {title_case(key) if isinstance(key, str) else key: value for key, value in data.items()}
