import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pydantic import TypeAdapter, ValidationError

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, NodeId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.resource_ios._datamodel import ContainerIO, DataModelIO, NodeIO, SpaceIO, ViewIO
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import YamlPosition
from cognite_toolkit._cdf_tk.validation import humanize_validation_error
from cognite_toolkit._cdf_tk.yaml_classes import ContainerYAML, DataModelYAML, NodeYAML, SpaceYAML, ViewYAML
from cognite_toolkit._cdf_tk.yaml_classes.container_field_definitions import (
    ConstraintType,
    ContainerPropertyDefinition,
    IndexDefinition,
    IndexType,
    UniquenessConstraintDefinition,
)
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatYAML
from cognite_toolkit._cdf_tk.yaml_classes.neat._constant import (
    SHEET_CONTAINERS,
    SHEET_METADATA,
    SHEET_NODES,
    SHEET_PROPERTIES,
    SHEET_VIEWS,
)
from cognite_toolkit._cdf_tk.yaml_classes.neat._entities import ParsedEntity, parse_entity
from cognite_toolkit._cdf_tk.yaml_classes.neat._table import (
    DMSContainer,
    DMSEnum,
    DMSNode,
    DMSProperty,
    DMSView,
    EntityTableFilter,
    RAWFilterTableFilter,
    TableViewFilter,
)
from cognite_toolkit._cdf_tk.yaml_classes.neat._table_source import NeatTableSource
from cognite_toolkit._cdf_tk.yaml_classes.view_field_definitions import ViewPropertyType

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
        BuildError,
        BuildWarning,
        InsightCode,
    )


# A spreadsheet cell holding this value is treated the same as an empty cell. See NeatTableReader.read
# for details.
CELL_MISSING = "#N/A"

# A container reference is identified by (space, external_id).
ContainerRef = tuple[str, str]
# A view reference is identified by (space, external_id, version).
ViewRef = tuple[str, str, str]

_VIEW_PROPERTY_ADAPTER: TypeAdapter[ViewPropertyType] = TypeAdapter(ViewPropertyType)
_CONSTRAINT_ADAPTER: TypeAdapter[ConstraintType] = TypeAdapter(ConstraintType)
_INDEX_ADAPTER: TypeAdapter[IndexType] = TypeAdapter(IndexType)


@dataclass
class NeatReadResult:
    space: CrudResource[SpaceId]
    containers: list[CrudResource[ContainerId]]
    views: list[CrudResource[ViewId]]
    node_types: list[CrudResource[NodeId]]
    data_model: CrudResource[DataModelId]


class NeatTableError(Exception):
    """Raised when the NEAT table cannot be converted at all.

    This is only raised for issues that make it impossible to produce any output, for example an
    invalid space or an invalid data model. Everything else is reported as a warning (see
    ``NeatTableReader.get_warnings``), and the offending row/property/container/view is skipped.
    """


@dataclass
class _ReadContainerProperty:
    prop_id: str
    row_no: int
    container_ref: ContainerRef
    definition: ContainerPropertyDefinition


@dataclass
class _ReadViewProperty:
    prop_id: str
    row_no: int
    view_ref: ViewRef
    definition: ViewPropertyType


_T = TypeVar("_T")


@dataclass
class _ReadOrdered(Generic[_T]):
    """A read index or (uniqueness) constraint entry, which may need to be ordered relative to its
    siblings when it spans multiple properties."""

    id_: str
    prop_id: str
    order: int | None
    row_no: int
    definition: _T


class NeatTableReader:
    """Converts a NEAT table into Toolkit's YAML resources"""

    def __init__(self, neat_yaml: NeatYAML, source: NeatTableSource):
        self.neat_yaml = neat_yaml
        self.source = source
        self.default_space = neat_yaml.metadata.space
        self.default_version = neat_yaml.metadata.version
        self.governed_spaces = set(neat_yaml.metadata.governed_spaces or []) | {neat_yaml.metadata.space}
        self._warnings: list[BuildWarning] = []
        self._errors: list[BuildError] = []

    def get_warnings(self) -> "list[BuildWarning]":
        return self._warnings

    def get_errors(self) -> "list[BuildError]":
        return self._errors

    def read(self) -> NeatReadResult:
        tables = self.neat_yaml
        space = self._read_space()
        enum_collections = self._read_enum_collections(tables.enum)
        container_refs = self._container_refs(tables.containers)
        view_refs = self._view_refs(tables.views)

        container_props, view_props, indices, constraints = self._read_properties(
            tables.properties, enum_collections, container_refs, view_refs
        )

        containers = self._read_containers(tables.containers, container_props, indices, constraints)
        views, valid_view_refs = self._read_views(tables.views, view_props)
        node_types = self._read_nodes(tables.nodes)
        data_model = self._read_data_model(tables.views, valid_view_refs)

        return NeatReadResult(
            space=space,
            containers=containers,
            views=views,
            node_types=node_types,
            data_model=data_model,
        )

    # --- Warnings & locations -------------------------------------------------------------------

    def _create_error(
        self,
        loc: tuple[int | str, ...],
        validation_error: ValidationError | str,
        validation_type: Any | None = None,
        code: "InsightCode" = "INVALID-VALUE",
    ) -> None:
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError

        if isinstance(validation_error, ValidationError):
            errors = humanize_validation_error(validation_error, validation_type)
            message = "\n".join(item for item in errors)
        else:
            message = validation_error
        location = self.source.location(loc)
        if isinstance(location, str):
            message = f"{location}: {message}"
        self._errors.append(
            BuildError(
                code=code,
                message=message,
                source_file=self.source.source,
                line=location.line if isinstance(location, YamlPosition) else None,
                column=location.column if isinstance(location, YamlPosition) else None,
                alpha=Flags.NEAT.is_enabled(),
                fix="Compare the YAML with reference documentation and make sure it is valid.",
            )
        )

    def _create_warning(self, code: "InsightCode", message: str, loc: tuple[int | str, ...]) -> None:
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning

        location = self.source.location(loc)
        if isinstance(location, str):
            message = f"{location}: {message}"

        self._warnings.append(
            BuildWarning(
                code=code,
                message=message,
                source_file=Path(self.source.source),
                line=location.line if isinstance(location, YamlPosition) else None,
                column=location.column if isinstance(location, YamlPosition) else None,
                alpha=Flags.NEAT.is_enabled(),
            )
        )

    # --- Entity -> reference helpers -------------------------------------------------------------

    def _container_ref_key(self, entity: ParsedEntity | None) -> ContainerRef | None:
        if entity is None or not entity.suffix or entity.suffix == CELL_MISSING:
            return None
        return entity.prefix or self.default_space, entity.suffix

    def _view_ref_key(self, entity: ParsedEntity | None) -> ViewRef | None:
        if entity is None or not entity.suffix or entity.suffix == CELL_MISSING:
            return None
        space = entity.prefix or self.default_space
        version = entity.properties.get("version") or self.default_version
        return space, entity.suffix, version

    def _container_ref_dict(self, entity: ParsedEntity | None) -> dict[str, Any]:
        if (ref := self._container_ref_key(entity)) is None:
            return {}
        return {"type": "container", "space": ref[0], "externalId": ref[1]}

    def _view_ref_dict(self, entity: ParsedEntity | None) -> dict[str, Any]:
        if (ref := self._view_ref_key(entity)) is None:
            return {}
        return {"type": "view", "space": ref[0], "externalId": ref[1], "version": ref[2]}

    def _node_ref_dict(
        self, entity: ParsedEntity | None, *, view: ParsedEntity | None = None, view_property: str | None = None
    ) -> dict[str, Any]:
        if entity is None or not entity.suffix or entity.suffix == CELL_MISSING:
            if view is None or view_property is None:
                return {}
            # Fall back to a node named after the view and property, matching the Neat convention
            # for edge types that are not explicitly given.
            return {"space": view.prefix or self.default_space, "externalId": f"{view.suffix}.{view_property}"}
        return {"space": entity.prefix or self.default_space, "externalId": entity.suffix}

    def _parse_entity_str(self, value: str, loc: tuple[int | str, ...]) -> ParsedEntity | None:
        try:
            return parse_entity(value)
        except ValueError as e:
            self._create_error(loc, f"Failed to parse {value!r}: {e!s}", code="INVALID-VALUE")
            return None

    def _container_refs(self, containers: list[DMSContainer]) -> set[ContainerRef]:
        return {ref for container in containers if (ref := self._container_ref_key(container.container)) is not None}

    def _view_refs(self, views: list[DMSView]) -> set[ViewRef]:
        return {ref for view in views if (ref := self._view_ref_key(view.view)) is not None}

    # --- Space -----------------------------------------------------------------------------------
    def _read_space(self) -> CrudResource[SpaceId]:
        try:
            space = SpaceYAML.model_validate({"space": self.neat_yaml.metadata.space})
        except ValidationError as e:
            self._create_error((SHEET_METADATA, "space"), e, SpaceYAML)
            raise NeatTableError("Invalid space")
        return CrudResource(
            raw=space.model_dump(exclude_unset=True),
            identifier=SpaceId(space=self.neat_yaml.metadata.space),
            type_=SpaceIO.as_crud_type(),
            validated=space,
        )

    # --- Enum ------------------------------------------------------------------------------------

    @staticmethod
    def _read_enum_collections(rows: list[DMSEnum]) -> dict[str, dict[str, dict[str, Any]]]:
        collections: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for row in rows:
            value = {"name": row.name, "description": row.description}
            collections[row.collection][row.value] = {key: item for key, item in value.items() if item is not None}
        return collections

    # --- Properties --------------------------------------------------------------------------------

    def _read_properties(
        self,
        properties: list[DMSProperty],
        enum_collections: dict[str, dict[str, dict[str, Any]]],
        container_refs: set[ContainerRef],
        view_refs: set[ViewRef],
    ) -> tuple[
        dict[ContainerRef, dict[str, ContainerPropertyDefinition]],
        dict[ViewRef, dict[str, ViewPropertyType]],
        dict[ContainerRef, dict[str, IndexType]],
        dict[ContainerRef, dict[str, ConstraintType]],
    ]:
        container_reads: dict[tuple[ContainerRef, str], list[_ReadContainerProperty]] = defaultdict(list)
        view_reads: dict[tuple[ViewRef, str], list[_ReadViewProperty]] = defaultdict(list)
        index_reads: dict[tuple[ContainerRef, str], list[_ReadOrdered[IndexType]]] = defaultdict(list)
        constraint_reads: dict[tuple[ContainerRef, str], list[_ReadOrdered[ConstraintType]]] = defaultdict(list)

        for row_no, prop in enumerate(properties):
            view_property = self._read_view_property(prop, row_no, view_refs)
            if view_property is not None:
                view_reads[(view_property.view_ref, view_property.prop_id)].append(view_property)

            if prop.container is None or prop.container_property is None:
                # This is an edge or reverse direct relation property, which has no container mapping.
                continue

            container_property = self._read_container_property(prop, row_no, enum_collections, container_refs)
            if container_property is None:
                continue
            container_ref = container_property.container_ref
            container_reads[(container_ref, container_property.prop_id)].append(container_property)
            if prop.index:
                indexes = self._process_ordered(prop.index, prop.container_property, row_no, "Index", _INDEX_ADAPTER)
                for index in indexes:
                    index_reads[(container_ref, index.id_)].append(index)
            if prop.constraint:
                constraints = self._process_ordered(
                    prop.constraint, prop.container_property, row_no, "Constraint", _CONSTRAINT_ADAPTER
                )
                for constraint in constraints:
                    constraint_reads[(container_ref, constraint.id_)].append(constraint)

        container_props = self._finalize_container_properties(container_reads)
        view_props = self._finalize_view_properties(view_reads)
        indices = self._finalize_ordered(index_reads, "index", "Index")
        constraints = self._finalize_ordered(constraint_reads, "constraint", "Constraint")
        return container_props, view_props, indices, constraints

    def _read_view_property(
        self,
        prop: DMSProperty,
        row_no: int,
        view_refs: set[ViewRef],
    ) -> _ReadViewProperty | None:
        data = self._build_view_property(prop, row_no)
        if data is None:
            return None
        try:
            view_property = _VIEW_PROPERTY_ADAPTER.validate_python(data)
        except ValidationError as e:
            self._create_error((SHEET_PROPERTIES, row_no), e, _VIEW_PROPERTY_ADAPTER, "INVALID-VALUE")
            return None
        view_ref = self._view_ref_key(prop.view)
        if view_ref is None:
            self._create_error((SHEET_PROPERTIES, row_no, "View"), "Invalid view reference", code="INVALID-VALUE")
            return None
        if view_ref not in view_refs:
            self._create_error(
                (SHEET_PROPERTIES, row_no, "View"),
                f"the view {prop.view!s} was not found in the {SHEET_VIEWS!r} table.",
                code="MISSING-REFERENCED-RESOURCE",
            )
            return None
        return _ReadViewProperty(prop_id=prop.view_property, row_no=row_no, view_ref=view_ref, definition=view_property)

    def _build_view_property(self, prop: DMSProperty, row_no: int) -> dict[str, Any] | None:
        if prop.connection is None or prop.connection.suffix == "direct":
            return self._build_core_view_property(prop)
        elif prop.connection.suffix == "edge":
            return self._build_edge_view_property(prop, row_no)
        elif prop.connection.suffix == "reverse":
            return self._build_reverse_view_property(prop, row_no)
        self._create_error(
            (SHEET_PROPERTIES, row_no, "Connection"),
            f"invalid connection type {prop.connection.suffix!r}",
            code="INVALID-VALUE",
        )
        return None

    def _build_core_view_property(self, prop: DMSProperty) -> dict[str, Any]:
        source = None
        if prop.connection is not None and prop.value_type.suffix != CELL_MISSING:
            source = self._view_ref_dict(prop.value_type)
        return {
            "name": prop.name,
            "description": prop.description,
            "container": self._container_ref_dict(prop.container),
            "containerPropertyIdentifier": prop.container_property,
            "source": source,
        }

    def _build_edge_view_property(self, prop: DMSProperty, row_no: int) -> dict[str, Any]:
        assert prop.connection is not None
        edge_source = None
        if raw_edge_source := prop.connection.properties.get("edgeSource"):
            if (
                entity := self._parse_entity_str(raw_edge_source, (SHEET_PROPERTIES, row_no, "Connection"))
            ) is not None:
                edge_source = self._view_ref_dict(entity)

        if raw_type := prop.connection.properties.get("type"):
            type_entity = self._parse_entity_str(raw_type, (SHEET_PROPERTIES, row_no, "Connection"))
            type_ref = self._node_ref_dict(type_entity) if type_entity is not None else {}
        else:
            type_ref = self._node_ref_dict(None, view=prop.view, view_property=prop.view_property)

        return {
            "connectionType": "single_edge_connection" if prop.max_count == 1 else "multi_edge_connection",
            "name": prop.name,
            "description": prop.description,
            "source": self._view_ref_dict(prop.value_type),
            "type": type_ref,
            "edgeSource": edge_source,
            "direction": prop.connection.properties.get("direction", "outwards"),
        }

    def _build_reverse_view_property(self, prop: DMSProperty, row_no: int) -> dict[str, Any]:
        assert prop.connection is not None
        view_ref = self._view_ref_dict(prop.value_type)
        return {
            "connectionType": (
                "single_reverse_direct_relation" if prop.max_count == 1 else "multi_reverse_direct_relation"
            ),
            "name": prop.name,
            "description": prop.description,
            "source": view_ref,
            "through": {"source": dict(view_ref), "identifier": prop.connection.properties.get("property")},
        }

    def _read_container_property(
        self,
        prop: DMSProperty,
        row_no: int,
        enum_collections: dict[str, dict[str, dict[str, Any]]],
        container_refs: set[ContainerRef],
    ) -> _ReadContainerProperty | None:
        assert prop.container_property is not None
        data = self._build_container_property(prop, enum_collections, row_no)
        try:
            definition = ContainerPropertyDefinition.model_validate(data)
        except ValidationError as e:
            self._create_error((SHEET_PROPERTIES, row_no), e, ContainerPropertyDefinition, "INVALID-VALUE")
            return None
        container_ref = self._container_ref_key(prop.container)
        if container_ref is None:
            self._create_error(
                (SHEET_PROPERTIES, row_no, "Container"),
                f"the container reference {prop.container!s} is invalid.",
                code="INVALID-VALUE",
            )
            return None
        if container_ref not in container_refs:
            if container_ref[0] in self.governed_spaces:
                # Containers in governed spaces must be defined in the Containers table. You can
                # reference containers in non-governed spaces without defining them.
                self._create_error(
                    (SHEET_PROPERTIES, row_no, "Container"),
                    f"the container {prop.container!s} was not found in the {SHEET_CONTAINERS!r} table.",
                    code="MISSING-REFERENCED-RESOURCE",
                )
            return None
        return _ReadContainerProperty(prop.container_property, row_no, container_ref, definition)

    def _build_container_property(
        self, prop: DMSProperty, enum_collections: dict[str, dict[str, dict[str, Any]]], row_no: int
    ) -> dict[str, Any]:
        return {
            "immutable": prop.immutable,
            "nullable": prop.min_count == 0 or prop.min_count is None,
            "autoIncrement": prop.auto_increment,
            "defaultValue": prop.default,
            "description": prop.container_property_description,
            "name": prop.container_property_name,
            "type": self._build_data_type(prop, enum_collections, row_no),
        }

    def _build_data_type(
        self, prop: DMSProperty, enum_collections: dict[str, dict[str, dict[str, Any]]], row_no: int
    ) -> dict[str, Any]:
        value_type = "direct" if prop.connection is not None else prop.value_type.suffix
        args: dict[str, Any] = {"type": value_type}
        if value_type != "enum":
            # Every property type except 'enum' supports being a list. 'enum' has no 'list'/'maxListSize'
            # fields, so these keys must not be present at all (extra fields are forbidden).
            is_list = None if prop.max_count is None else prop.max_count > 1
            args["list"] = is_list
            args["maxListSize"] = prop.max_count if is_list else None
        args.update(prop.value_type.properties)

        if prop.connection is not None:
            # A direct relation's container constraint is given on the 'Connection' column, for example
            # 'direct(container=my_space:Equipment)', not on the 'Value Type' column.
            args.pop("container", None)
            if raw_container := prop.connection.properties.get("container"):
                entity = self._parse_entity_str(raw_container, (SHEET_PROPERTIES, row_no, "Connection"))
                if entity is not None:
                    args["container"] = self._container_ref_dict(entity)

        if value_type == "enum" and "collection" in prop.value_type.properties:
            # 'collection' is a table-only convention used to look up the Enum sheet; it is not a
            # recognized field on the enum property type itself.
            args["values"] = enum_collections.get(args.pop("collection"), {})
        return args

    def _process_ordered(
        self,
        entities: list[ParsedEntity],
        prop_id: str,
        row_no: int,
        column: str,
        adapter: TypeAdapter[_T],
    ) -> list[_ReadOrdered[_T]]:
        key_name = "indexType" if column == "Index" else "constraintType"
        indexes: list[_ReadOrdered[_T]] = []
        for entity in entities:
            # 'order' is only used to sort multi-property indices/constraints below; it is not a
            # recognized field on the index/constraint definition itself, so it must not be passed through.
            extra_properties = dict(entity.properties)
            order_raw = extra_properties.pop("order", None)
            data = {key_name: entity.prefix, "properties": [prop_id], **extra_properties}
            try:
                definition = adapter.validate_python(data)
            except ValidationError as e:
                self._create_error((SHEET_PROPERTIES, row_no, column), e, adapter, "INVALID-VALUE")
                continue
            order: int | None = None
            if order_raw is not None:
                order = self._read_order(order_raw, (SHEET_PROPERTIES, row_no, column))
                if order is None:
                    continue
            indexes.append(_ReadOrdered(entity.suffix, prop_id, order, row_no, definition))
        return indexes

    def _read_order(self, order_raw: str, loc: tuple[int | str, ...]) -> int | None:
        try:
            return int(order_raw)
        except ValueError:
            self._create_error(loc, f"invalid order value {order_raw!r}, must be an integer.", code="INVALID-VALUE")
            return None

    def _finalize_container_properties(
        self, reads: dict[tuple[ContainerRef, str], list[_ReadContainerProperty]]
    ) -> dict[ContainerRef, dict[str, ContainerPropertyDefinition]]:
        result: dict[ContainerRef, dict[str, ContainerPropertyDefinition]] = defaultdict(dict)
        for (container_ref, prop_id), entries in reads.items():
            result[container_ref][prop_id] = entries[0].definition
            if len(entries) > 1 and any(entry.definition != entries[0].definition for entry in entries[1:]):
                rows = humanize_collection(
                    [self.source.adjust_row_number(SHEET_PROPERTIES, entry.row_no) for entry in entries]
                )
                self._create_warning(
                    "INVALID-VALUE",
                    f"Multiple properties map to container {container_ref[0]}:{container_ref[1]} property "
                    f"{prop_id!r} with different definitions. Using the first definition found, from rows: {rows}.",
                    (SHEET_PROPERTIES,),
                )
        return result

    def _finalize_view_properties(
        self, reads: dict[tuple[ViewRef, str], list[_ReadViewProperty]]
    ) -> dict[ViewRef, dict[str, Any]]:
        result: dict[ViewRef, dict[str, Any]] = defaultdict(dict)
        for (view_ref, prop_id), entries in reads.items():
            result[view_ref][prop_id] = entries[0].definition
            if len(entries) > 1:
                rows = humanize_collection(
                    [self.source.adjust_row_number(SHEET_PROPERTIES, entry.row_no) for entry in entries]
                )
                self._create_error(
                    (SHEET_PROPERTIES,),
                    f"The combination of 'View' and 'View Property' must be unique. Duplicated entries for view "
                    f"{view_ref[0]}:{view_ref[1]}(version={view_ref[2]}) property {prop_id!r} found in rows: {rows}.",
                    code="INVALID-VALUE",
                )
        return result

    def _finalize_ordered(
        self,
        reads: dict[tuple[ContainerRef, str], list[_ReadOrdered[_T]]],
        kind: str,
        column: str,
    ) -> dict[ContainerRef, dict[str, _T]]:
        result: dict[ContainerRef, dict[str, _T]] = defaultdict(dict)
        for (container_ref, identifier), entries in reads.items():
            # Deduplicate by property identifier, keeping the first occurrence.
            unique_entries = list({entry.prop_id: entry for entry in entries}.values())
            definition = unique_entries[0].definition
            if len(unique_entries) == 1 or not isinstance(definition, IndexDefinition | UniquenessConstraintDefinition):
                result[container_ref][identifier] = definition
                continue
            if missing_order := [entry for entry in unique_entries if entry.order is None]:
                rows = humanize_collection(
                    [self.source.adjust_row_number(SHEET_PROPERTIES, entry.row_no) for entry in missing_order]
                )
                self._create_warning(
                    "INVALID-VALUE",
                    f"The {kind} {identifier!r} on container {container_ref[0]}:{container_ref[1]} is defined on "
                    f"multiple properties and requires the 'order' property to be set. It is missing in rows: {rows}.",
                    (SHEET_PROPERTIES, column),
                )
                continue
            definition.properties = [entry.prop_id for entry in sorted(unique_entries, key=lambda e: e.order or 999)]
            result[container_ref][identifier] = definition
        return result

    # --- Containers --------------------------------------------------------------------------------

    def _read_containers(
        self,
        containers: list[DMSContainer],
        container_props: dict[ContainerRef, dict[str, ContainerPropertyDefinition]],
        indices: dict[ContainerRef, dict[str, IndexType]],
        constraints: dict[ContainerRef, dict[str, ConstraintType]],
    ) -> list[CrudResource[ContainerId]]:
        results: list[CrudResource[ContainerId]] = []
        rows_by_ref: dict[ContainerRef, list[int]] = defaultdict(list)

        for row_no, container in enumerate(containers):
            ref = self._container_ref_key(container.container)
            if ref is None:
                self._create_error(
                    (SHEET_CONTAINERS, row_no, "Container"), "Invalid container reference.", code="INVALID-VALUE"
                )
                continue

            property_constraints = constraints.get(ref, {})
            container_constraints = self._read_container_constraints(container, row_no)
            if conflict := set(property_constraints) & set(container_constraints):
                self._create_warning(
                    "INVALID-VALUE",
                    f"container {ref[0]}:{ref[1]} has "
                    f"constraint identifier(s) that conflict with a uniqueness constraint defined in the "
                    f"{SHEET_PROPERTIES!r} table: {humanize_collection(conflict)}.",
                    (SHEET_CONTAINERS, row_no, "Constraint"),
                )
            merged_constraints = {**property_constraints, **container_constraints}

            data: dict[str, Any] = {
                "space": ref[0],
                "externalId": ref[1],
                "usedFor": container.used_for,
                "name": container.name,
                "description": container.description,
                "properties": container_props.get(ref, {}),
                "indexes": indices.get(ref) or None,
                "constraints": merged_constraints or None,
            }
            try:
                container_yaml = ContainerYAML.model_validate(data)
            except ValidationError as e:
                self._create_error((SHEET_CONTAINERS, row_no, "Container"), e, ContainerYAML, "INVALID-VALUE")
                continue

            rows_by_ref[ref].append(row_no)
            if len(rows_by_ref[ref]) > 1:
                rows = humanize_collection(
                    [self.source.adjust_row_number(SHEET_CONTAINERS, r) for r in rows_by_ref[ref]]
                )
                self._create_error(
                    (SHEET_CONTAINERS, "Container"),
                    f"The values in column 'Container' must be unique. Duplicated entries for container "
                    f"{ref[0]}:{ref[1]} found in rows: {rows}.",
                    code="INVALID-VALUE",
                )
                continue

            results.append(
                CrudResource(
                    raw=container_yaml.model_dump(by_alias=True, exclude_none=True),
                    identifier=container_yaml.as_id(),
                    type_=ContainerIO.as_crud_type(),
                    validated=container_yaml,
                )
            )
        return results

    def _read_container_constraints(self, container: DMSContainer, row_no: int) -> dict[str, ConstraintType]:
        result: dict[str, ConstraintType] = {}
        for entity in container.constraint or []:
            if entity.prefix != "requires":
                self._create_error(
                    (SHEET_CONTAINERS, row_no, "Constraint"),
                    f"The constraint {entity.suffix!r} has an invalid type {entity.prefix!r}. Only "
                    "'requires' constraints are supported at the container level.",
                    code="INVALID-VALUE",
                )
                continue
            if "require" not in entity.properties:
                self._create_error(
                    (SHEET_CONTAINERS, row_no, "Constraint"),
                    f"The constraint {entity.suffix!r} is missing the 'require' property, which is "
                    "required for container-level constraints.",
                    code="INVALID-VALUE",
                )
                continue
            required_entity = self._parse_entity_str(
                entity.properties["require"], (SHEET_CONTAINERS, row_no, "Constraint")
            )
            data = {
                "constraintType": "requires",
                "require": self._container_ref_dict(required_entity) if required_entity is not None else {},
            }
            try:
                definition = _CONSTRAINT_ADAPTER.validate_python(data)
            except ValidationError as e:
                self._create_error((SHEET_CONTAINERS, row_no, "Constraint"), e, _CONSTRAINT_ADAPTER, "INVALID-VALUE")
                continue
            result[entity.suffix] = definition
        return result

    # --- Views -------------------------------------------------------------------------------------

    def _read_views(
        self, views: list[DMSView], view_props: dict[ViewRef, dict[str, ViewPropertyType]]
    ) -> tuple[list[CrudResource[ViewId]], set[ViewRef]]:
        results: list[CrudResource[ViewId]] = []
        rows_by_ref: dict[ViewRef, list[int]] = defaultdict(list)
        valid_refs: set[ViewRef] = set()

        for row_no, view in enumerate(views):
            ref = self._view_ref_key(view.view)
            if ref is None:
                self._create_error((SHEET_VIEWS, row_no, "View"), "invalid view reference.", code="INVALID-VALUE")
                continue

            implements = [self._view_ref_dict(entity) for entity in view.implements] if view.implements else None
            filter_dict = self._build_filter_dict(view.filter, row_no) if view.filter is not None else None

            data: dict[str, Any] = {
                "space": ref[0],
                "externalId": ref[1],
                "version": ref[2],
                "name": view.name,
                "description": view.description,
                "implements": implements,
                "filter": filter_dict,
                "properties": view_props.get(ref) or None,
            }
            try:
                view_yaml = ViewYAML.model_validate(data)
            except ValidationError as e:
                self._create_error((SHEET_VIEWS, row_no), e, ViewYAML, "INVALID-VALUE")
                continue

            rows_by_ref[ref].append(row_no)
            if len(rows_by_ref[ref]) > 1:
                rows = humanize_collection([self.source.adjust_row_number(SHEET_VIEWS, r) for r in rows_by_ref[ref]])
                self._create_error(
                    (SHEET_VIEWS, row_no, "View"),
                    f"The values in column 'View' must be unique. Duplicated entries for view "
                    f"{ref[0]}:{ref[1]}(version={ref[2]}) found in rows: {rows}.",
                    code="INVALID-VALUE",
                )
                continue

            valid_refs.add(ref)
            results.append(
                CrudResource(
                    raw=view_yaml.model_dump(by_alias=True, exclude_none=True),
                    identifier=view_yaml.as_id(),
                    type_=ViewIO.as_crud_type(),
                    validated=view_yaml,
                )
            )
        return results, valid_refs

    def _build_filter_dict(self, filter_: TableViewFilter, row_no: int) -> dict[str, Any] | None:
        if isinstance(filter_, RAWFilterTableFilter):
            try:
                return json.loads(filter_.filter)
            except ValueError as e:
                self._create_error(
                    (SHEET_VIEWS, row_no, "Filter"),
                    f"filter must be valid JSON. Got error: {e!s}",
                    code="INVALID-VALUE",
                )
                return None
        if not isinstance(filter_, EntityTableFilter):
            # Unreachable: TableViewFilter is a discriminated union of these two types.
            raise RuntimeError(f"Unknown filter type {type(filter_).__name__}")
        if filter_.type == "hasData":
            return {"hasData": [self._container_ref_dict(entity) for entity in filter_.entities]}
        node_refs = [self._node_ref_dict(entity) for entity in filter_.entities]
        if len(node_refs) == 1:
            return {"equals": {"property": ["node", "type"], "value": node_refs[0]}}
        return {"in": {"property": ["node", "type"], "values": node_refs}}

    # --- Nodes -------------------------------------------------------------------------------------

    def _read_nodes(self, nodes: list[DMSNode]) -> list[CrudResource[NodeId]]:
        results: list[CrudResource[NodeId]] = []
        for row_no, node in enumerate(nodes):
            node_ref = self._node_ref_dict(node.node)
            if not node_ref:
                self._create_error((SHEET_NODES, row_no, "Node"), "invalid node reference.", code="INVALID-VALUE")
                continue
            try:
                node_yaml = NodeYAML.model_validate(node_ref)
            except ValidationError as e:
                self._create_error((SHEET_NODES, row_no, "Node"), e, NodeYAML, "INVALID-VALUE")
                continue
            results.append(
                CrudResource(
                    raw=node_yaml.model_dump(by_alias=True, exclude_none=True),
                    identifier=node_yaml.as_id(),
                    type_=NodeIO.as_crud_type(),
                    validated=node_yaml,
                )
            )
        return results

    # --- Data model ----------------------------------------------------------------------------------

    def _read_data_model(self, views: list[DMSView], valid_view_refs: set[ViewRef]) -> CrudResource[DataModelId]:
        metadata = self.neat_yaml.metadata

        seen: set[ViewRef] = set()
        view_refs: list[dict[str, Any]] = []
        for view in views:
            ref = self._view_ref_key(view.view)
            if ref is None or ref not in valid_view_refs or ref in seen:
                continue
            seen.add(ref)
            view_refs.append({"type": "view", "space": ref[0], "externalId": ref[1], "version": ref[2]})

        data: dict[str, Any] = {
            "space": metadata.space,
            "externalId": metadata.external_id,
            "version": metadata.version,
            "name": metadata.name,
            "description": metadata.description,
            "views": view_refs,
        }
        try:
            data_model_yaml = DataModelYAML.model_validate(data)
        except ValidationError as e:
            self._create_error((SHEET_METADATA,), e, DataModelYAML, "INVALID-VALUE")
            raise NeatTableError("Invalid data model") from None
        return CrudResource(
            raw=data_model_yaml.model_dump(by_alias=True, exclude_none=True),
            identifier=data_model_yaml.as_id(),
            type_=DataModelIO.as_crud_type(),
            validated=data_model_yaml,
        )
