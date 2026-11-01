import json
import re
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any, final

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, NodeId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerRequest,
    DataModelRequest,
    NodeRequest,
    SpaceRequest,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import ResourceFileIO, ResourceIO, ResourceType
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.yaml_classes import NeatYAML

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError, BuildWarning


@final
class NeatIO(ResourceFileIO[DataModelId, NeatYAML]):
    folder_name = "data_modeling"
    kind = "Neat"
    yaml_cls = NeatYAML

    @classmethod
    def get_id(cls, item: dict) -> DataModelId:
        metadata = _metadata(item)
        return DataModelId(
            space=_required_metadata(metadata, "space"),
            external_id=_required_metadata(metadata, "externalId"),
            version=_required_metadata(metadata, "version"),
        )

    @classmethod
    def as_crud_type(cls) -> ResourceType:
        raise RuntimeError("Bug in Toolkit: NeatIO is multiple crud types, use the to_crud_types methods instead")

    @classmethod
    def get_dependencies(cls, resource: NeatYAML) -> "Iterable[tuple[type[ResourceIO], Identifier]]":
        return []

    @classmethod
    def to_crud_types(
        cls, raw: dict[str, Any], source_file: Path, resource: NeatYAML | None = None
    ) -> "Iterable[CrudResource[Identifier] | BuildError | BuildWarning]":
        if resource is None:
            from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError

            yield BuildError(
                code="INVALID-VALUE",
                message="Neat requires valid YAML syntax to be converted to API format.",
                source_file=source_file,
                alpha=Flags.NEAT.is_enabled(),
            )
            return
        try:
            converted = _NeatTableConverter(raw).convert()
        except NeatTableError as error:
            from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError

            yield BuildError(
                code="INVALID-VALUE",
                message=str(error),
                source_file=source_file,
                alpha=Flags.NEAT.is_enabled(),
            )
            return

        from cognite_toolkit._cdf_tk.resource_ios._datamodel import ContainerIO, DataModelIO, NodeIO, SpaceIO, ViewIO

        space = converted.space
        yield CrudResource(
            raw=space,
            identifier=SpaceId(space=space["space"]),
            type_=ResourceType(resource_folder=SpaceIO.folder_name, kind=SpaceIO.kind),
        )
        for container in converted.containers:
            yield CrudResource(
                raw=container,
                identifier=ContainerId(space=container["space"], external_id=container["externalId"]),
                type_=ResourceType(resource_folder=ContainerIO.folder_name, kind=ContainerIO.kind),
            )
        for view in converted.views:
            yield CrudResource(
                raw=view,
                identifier=ViewId(space=view["space"], external_id=view["externalId"], version=view["version"]),
                type_=ResourceType(resource_folder=ViewIO.folder_name, kind=ViewIO.kind),
            )
        for node in converted.nodes:
            yield CrudResource(
                raw=node,
                identifier=NodeId(space=node["space"], external_id=node["externalId"]),
                type_=ResourceType(resource_folder=NodeIO.folder_name, kind=NodeIO.kind),
            )
        yield CrudResource(
            raw=converted.data_model,
            identifier=cls.get_id(raw),
            type_=ResourceType(resource_folder=DataModelIO.folder_name, kind=DataModelIO.kind),
        )

    @classmethod
    def from_crud_types(cls, resources: Iterable[CrudResource[Identifier]]) -> dict[str, Any]:
        raise RuntimeError("Bug in Toolkit: Neat does not support conversion from API format to YAML format.")


class NeatTableError(ValueError):
    """An invalid value in a NEAT table."""


@final
class _ConvertedNeatTable:
    def __init__(
        self,
        space: dict[str, Any],
        containers: list[dict[str, Any]],
        views: list[dict[str, Any]],
        nodes: list[dict[str, Any]],
        data_model: dict[str, Any],
    ) -> None:
        self.space = space
        self.containers = containers
        self.views = views
        self.nodes = nodes
        self.data_model = data_model


@final
class _NeatTableConverter:
    """Convert NEAT's tabular YAML representation to DMS request resources."""

    _entity_re = re.compile(r"^(?:(?P<space>[^:(]+):)?(?P<external_id>[^()]+?)(?:\((?P<args>.*)\))?$")

    def __init__(self, raw: dict[str, Any]) -> None:
        self.raw = {str(key).title(): value for key, value in raw.items()}
        self.metadata = _metadata(self.raw)
        self.space_id = _required_metadata(self.metadata, "space")
        self.version = _required_metadata(self.metadata, "version")
        self.enums = self._read_enums()
        self.excluded_views = {
            self._view_key(self._view_ref(str(row["View"])))
            for row in self._table("Views")
            if _is_false(row.get("In Model")) and row.get("View") is not None
        }

    def convert(self) -> _ConvertedNeatTable:
        containers = self._read_containers()
        views = self._read_views()
        nodes = self._read_nodes()
        data_model = self._read_data_model(views)
        return _ConvertedNeatTable(
            space=SpaceRequest.model_validate({"space": self.space_id}).model_dump(by_alias=True, exclude_none=True),
            containers=containers,
            views=views,
            nodes=nodes,
            data_model=data_model,
        )

    def _read_enums(self) -> dict[str, dict[str, dict[str, Any]]]:
        collections: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for row in self._table("Enum"):
            collection = _required(row, "Collection", "Enum")
            value = _required(row, "Value", "Enum")
            collections[collection][value] = _without_none(
                {"name": row.get("Name"), "description": row.get("Description")}
            )
        return collections

    def _read_containers(self) -> list[dict[str, Any]]:
        definitions: dict[tuple[str, str], dict[str, Any]] = {}
        for row in self._table("Containers"):
            ref = self._container_ref(_required(row, "Container", "Containers"))
            key = (ref["space"], ref["externalId"])
            if key in definitions:
                raise NeatTableError(f"Containers table contains duplicate container {key[1]!r}.")
            definitions[key] = {
                **ref,
                "name": row.get("Name"),
                "description": row.get("Description"),
                "usedFor": row.get("Used For"),
                "properties": {},
                "indexes": {},
                "constraints": self._container_constraints(row.get("Constraint")),
            }

        for row in self._table("Properties"):
            container = row.get("Container")
            property_id = row.get("Container Property")
            if container is None or property_id is None:
                continue
            ref = self._container_ref(str(container))
            key = (ref["space"], ref["externalId"])
            if key not in definitions:
                raise NeatTableError(
                    f"Properties table references container {container!r}, which is not in Containers."
                )
            property_definition = self._container_property(row)
            existing = definitions[key]["properties"].get(str(property_id))
            if existing is not None and existing != property_definition:
                raise NeatTableError(
                    f"Container property {property_id!r} on {container!r} has inconsistent definitions."
                )
            definitions[key]["properties"][str(property_id)] = property_definition
            self._add_property_indexes(definitions[key]["indexes"], row.get("Index"), str(property_id))
            self._add_property_constraints(definitions[key]["constraints"], row.get("Constraint"), str(property_id))

        return [
            _validate_request(
                ContainerRequest, _clear_ordering(_without_none(item)), f"container {item['externalId']!r}"
            )
            for item in definitions.values()
        ]

    def _read_views(self) -> list[dict[str, Any]]:
        definitions: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in self._table("Views"):
            ref = self._view_ref(_required(row, "View", "Views"))
            key = self._view_key(ref)
            if key in self.excluded_views:
                continue
            if key in definitions:
                raise NeatTableError(f"Views table contains duplicate view {key[1]!r}.")
            definitions[key] = _without_none(
                {
                    **ref,
                    "name": row.get("Name"),
                    "description": row.get("Description"),
                    "implements": [self._view_ref(value) for value in _entity_list(row.get("Implements"))],
                    "filter": self._filter(row.get("Filter")),
                    "properties": {},
                }
            )

        for row in self._table("Properties"):
            view = _required(row, "View", "Properties")
            property_id = _required(row, "View Property", "Properties")
            ref = self._view_ref(view)
            key = self._view_key(ref)
            if key in self.excluded_views:
                continue
            if key not in definitions:
                raise NeatTableError(f"Properties table references view {view!r}, which is not in Views.")
            if property_id in definitions[key]["properties"]:
                raise NeatTableError(f"View property {property_id!r} is duplicated on view {view!r}.")
            definitions[key]["properties"][property_id] = self._view_property(row)

        return [_validate_request(ViewRequest, item, f"view {item['externalId']!r}") for item in definitions.values()]

    def _read_data_model(self, views: list[dict[str, Any]]) -> dict[str, Any]:
        data = {
            "space": self.space_id,
            "externalId": _required_metadata(self.metadata, "externalId"),
            "version": self.version,
            "name": self.metadata.get("name"),
            "description": self._description(),
            "views": [
                {"space": view["space"], "externalId": view["externalId"], "version": view["version"]} for view in views
            ],
        }
        return _validate_request(DataModelRequest, _without_none(data), "data model")

    def _read_nodes(self) -> list[dict[str, Any]]:
        nodes: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()
        for row in self._table("Nodes"):
            ref = self._node_ref(_required(row, "Node", "Nodes"))
            key = ref["space"], ref["externalId"]
            if key in seen:
                raise NeatTableError(f"Nodes table contains duplicate node {key[1]!r}.")
            seen.add(key)
            nodes.append(_validate_request(NodeRequest, {"instanceType": "node", **ref}, f"node {key[1]!r}"))
        return nodes

    def _container_property(self, row: dict[str, Any]) -> dict[str, Any]:
        value_type = self._entity(_required(row, "Value Type", "Properties"))
        connection = self._entity(row["Connection"]) if row.get("Connection") is not None else None
        is_list = row.get("Max Count") is None or _as_int(row.get("Max Count"), "Max Count") > 1
        type_data: dict[str, Any] = {
            "type": "direct" if connection is not None else value_type["externalId"],
            "list": is_list,
        }
        if is_list and row.get("Max Count") is not None:
            type_data["maxListSize"] = _as_int(row["Max Count"], "Max Count")
        type_data.update(value_type["args"])
        if connection and "container" in connection["args"]:
            type_data["container"] = self._container_ref(str(connection["args"]["container"]))
        if type_data["type"] == "enum":
            collection = str(value_type["args"].get("collection", ""))
            type_data["values"] = self.enums.get(collection, {})
        return _without_none(
            {
                "immutable": row.get("Immutable"),
                "nullable": row.get("Min Count") in (None, 0, "0"),
                "autoIncrement": row.get("Auto Increment"),
                "defaultValue": row.get("Default"),
                "name": row.get("Container Property Name"),
                "description": row.get("Container Property Description"),
                "type": type_data,
            }
        )

    def _view_property(self, row: dict[str, Any]) -> dict[str, Any]:
        connection = self._entity(row["Connection"]) if row.get("Connection") is not None else None
        value_type = self._view_ref(_required(row, "Value Type", "Properties"))
        common = {"name": row.get("Name"), "description": row.get("Description")}
        if connection is None or connection["externalId"] == "direct":
            return _without_none(
                {
                    **common,
                    "container": self._container_ref(row.get("Container")),
                    "containerPropertyIdentifier": row.get("Container Property"),
                    "source": value_type if connection is not None and row.get("Value Type") != "#N/A" else None,
                }
            )
        if connection["externalId"] == "reverse":
            return _without_none(
                {
                    **common,
                    "connectionType": (
                        "single_reverse_direct_relation"
                        if _as_int(row.get("Max Count"), "Max Count", default=0) == 1
                        else "multi_reverse_direct_relation"
                    ),
                    "source": value_type,
                    "through": {"source": value_type, "identifier": connection["args"].get("property")},
                }
            )
        if connection["externalId"] == "edge":
            view = self._view_ref(_required(row, "View", "Properties"))
            type_value = connection["args"].get("type") or f"{view['externalId']}.{row['View Property']}"
            return _without_none(
                {
                    **common,
                    "connectionType": (
                        "single_edge_connection"
                        if _as_int(row.get("Max Count"), "Max Count", default=0) == 1
                        else "multi_edge_connection"
                    ),
                    "source": value_type,
                    "edgeSource": self._view_ref(str(connection["args"]["edgeSource"]))
                    if connection["args"].get("edgeSource")
                    else None,
                    "type": self._node_ref(str(type_value)),
                    "direction": connection["args"].get("direction", "outwards"),
                }
            )
        raise NeatTableError(f"Unsupported connection type {connection['externalId']!r}.")

    def _container_constraints(self, value: Any) -> dict[str, Any]:
        constraints: dict[str, Any] = {}
        for constraint in _entity_list(value):
            entity = self._entity(constraint)
            if entity["prefix"] != "requires":
                raise NeatTableError("Only requires constraints are supported in the Containers table.")
            require = entity["args"].get("require")
            if not require:
                raise NeatTableError("A requires constraint needs a require value.")
            constraints[entity["externalId"]] = {
                "constraintType": "requires",
                "require": self._container_ref(str(require)),
            }
        return constraints

    def _add_property_indexes(self, indexes: dict[str, Any], value: Any, property_id: str) -> None:
        for index in _entity_list(value):
            entity = self._entity(index)
            index_data = {"indexType": entity["prefix"], "properties": [property_id], **entity["args"]}
            if not entity["prefix"]:
                raise NeatTableError(f"Index {index!r} is missing an index type.")
            self._append_ordered(indexes, entity["externalId"], index_data, property_id)

    def _add_property_constraints(self, constraints: dict[str, Any], value: Any, property_id: str) -> None:
        for constraint in _entity_list(value):
            entity = self._entity(constraint)
            constraint_data = {"constraintType": entity["prefix"], "properties": [property_id], **entity["args"]}
            if not entity["prefix"]:
                raise NeatTableError(f"Constraint {constraint!r} is missing a constraint type.")
            self._append_ordered(constraints, entity["externalId"], constraint_data, property_id)

    @staticmethod
    def _append_ordered(items: dict[str, Any], identifier: str, value: dict[str, Any], property_id: str) -> None:
        order = value.pop("order", None)
        if identifier not in items:
            items[identifier] = value
            if order is not None:
                items[identifier]["_ordered"] = [(_as_int(order, "order"), property_id)]
            return
        existing = items[identifier]
        if order is None:
            raise NeatTableError(f"{identifier!r} is defined on multiple properties but has no order.")
        if "_ordered" not in existing:
            existing["_ordered"] = [(0, existing["properties"][0])]
        existing["_ordered"].append((_as_int(order, "order"), property_id))
        existing["properties"] = [prop for _, prop in sorted(existing["_ordered"])]

    def _filter(self, value: Any) -> dict[str, Any] | None:
        if value is None:
            return None
        text = str(value)
        lowered = text.casefold()
        if lowered.startswith("rawfilter(") and text.endswith(")"):
            text = text[10:-1]
        if lowered.startswith("hasdata(") and text.endswith(")"):
            return {
                "hasData": [{**self._container_ref(item), "type": "container"} for item in _entity_list(text[8:-1])]
            }
        if lowered.startswith("nodetype(") and text.endswith(")"):
            nodes = [self._node_ref(item) for item in _entity_list(text[9:-1])]
            return (
                {"equals": {"property": ["node", "type"], "value": nodes[0]}}
                if len(nodes) == 1
                else {"in": {"property": ["node", "type"], "values": nodes}}
            )
        try:
            return json.loads(text)
        except json.JSONDecodeError as error:
            raise NeatTableError(f"View Filter must be valid JSON: {error.msg}.") from error

    def _description(self) -> str | None:
        description = self.metadata.get("description")
        creator = self.metadata.get("creator")
        if creator is None:
            return description
        creator_text = ", ".join(part.strip() for part in str(creator).split(",") if part.strip())
        return f"{description} Creator: {creator_text}" if description else f"Creator: {creator_text}"

    def _table(self, name: str) -> list[dict[str, Any]]:
        value = self.raw.get(name, [])
        if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
            raise NeatTableError(f"{name} must be a list of table rows.")
        return value

    def _entity(self, value: Any) -> dict[str, Any]:
        match = self._entity_re.fullmatch(str(value).strip())
        if not match:
            raise NeatTableError(f"Invalid NEAT entity {value!r}.")
        args = _arguments(match.group("args") or "")
        return {
            "prefix": match.group("space") or "",
            "space": match.group("space") or self.space_id,
            "externalId": match.group("external_id").strip(),
            "args": args,
        }

    def _container_ref(self, value: Any) -> dict[str, str]:
        if value is None:
            raise NeatTableError("A container reference is required.")
        entity = self._entity(value)
        return {"space": entity["space"], "externalId": entity["externalId"]}

    def _view_ref(self, value: Any) -> dict[str, str]:
        entity = self._entity(value)
        return {
            "space": entity["space"],
            "externalId": entity["externalId"],
            "version": str(entity["args"].get("version", self.version)),
        }

    def _node_ref(self, value: str) -> dict[str, str]:
        entity = self._entity(value)
        return {"space": entity["space"], "externalId": entity["externalId"]}

    @staticmethod
    def _view_key(reference: dict[str, str]) -> tuple[str, str, str]:
        return reference["space"], reference["externalId"], reference["version"]


def _metadata(raw: dict[str, Any]) -> dict[str, Any]:
    rows = raw.get("Metadata") or raw.get("metadata")
    if not isinstance(rows, list):
        raise NeatTableError("Metadata must be a list of Key/Value rows.")
    metadata: dict[str, Any] = {}
    for row in rows:
        if not isinstance(row, dict) or "Key" not in row or "Value" not in row:
            raise NeatTableError("Every Metadata row must contain Key and Value.")
        key = "externalId" if str(row["Key"]).lower() == "external_id" else str(row["Key"])
        metadata[key] = row["Value"]
    return metadata


def _required_metadata(metadata: dict[str, Any], key: str) -> str:
    if key not in metadata or metadata[key] is None:
        raise NeatTableError(f"Metadata table is missing required value {key!r}.")
    return str(metadata[key])


def _required(row: dict[str, Any], key: str, table: str) -> str:
    if key not in row or row[key] is None:
        raise NeatTableError(f"{table} table is missing required value {key!r}.")
    return str(row[key])


def _entity_list(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    return _split_top_level(str(value), ",")


def _arguments(value: str) -> dict[str, Any]:
    if not value:
        return {}
    arguments: dict[str, Any] = {}
    positional: list[str] = []
    for part in _split_top_level(value, ","):
        if "=" in part:
            key, raw_value = part.split("=", 1)
            arguments[key.strip()] = _scalar(raw_value.strip())
        else:
            positional.append(part.strip())
    if positional:
        arguments["_identifier"] = positional[0]
    return arguments


def _split_top_level(value: str, delimiter: str) -> list[str]:
    result: list[str] = []
    start = depth = 0
    quote: str | None = None
    for index, char in enumerate(value):
        if char in "'\"" and (index == 0 or value[index - 1] != "\\"):
            quote = None if quote == char else char if quote is None else quote
        elif quote is None and char == "(":
            depth += 1
        elif quote is None and char == ")":
            depth -= 1
        elif quote is None and depth == 0 and char == delimiter:
            result.append(value[start:index].strip())
            start = index + 1
    result.append(value[start:].strip())
    return [item for item in result if item]


def _scalar(value: str) -> Any:
    if value in {"True", "true"}:
        return True
    if value in {"False", "false"}:
        return False
    if value.startswith(("'", '"')) and value.endswith(("'", '"')):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        return value


def _is_false(value: Any) -> bool:
    return value is False or (isinstance(value, str) and value.casefold() == "false")


def _as_int(value: Any, name: str, default: int | None = None) -> int:
    if value is None and default is not None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as error:
        raise NeatTableError(f"{name} must be an integer.") from error


def _without_none(data: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in data.items() if value is not None}


def _clear_ordering(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _clear_ordering(item) for key, item in value.items() if key != "_ordered"}
    if isinstance(value, list):
        return [_clear_ordering(item) for item in value]
    return value


def _validate_request(request_cls: type[Any], data: dict[str, Any], name: str) -> dict[str, Any]:
    try:
        return request_cls.model_validate(data).model_dump(by_alias=True, exclude_none=True)
    except ValidationError as error:
        raise NeatTableError(f"Invalid {name}: {error.errors(include_url=False)[0]['msg']}.") from error
