import json
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter, ValidationError

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, NodeId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.resource_ios._datamodel import ContainerIO, DataModelIO, NodeIO, SpaceIO, ViewIO
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.validation import humanize_validation_error
from cognite_toolkit._cdf_tk.yaml_classes import ContainerYAML, DataModelYAML, NodeYAML, SpaceYAML, ViewYAML
from cognite_toolkit._cdf_tk.yaml_classes.container_field_definitions import (
    ConstraintType,
    ContainerPropertyDefinition,
    IndexType,
    UniquenessConstraintDefinition,
)
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatYAML
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
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError, BuildWarning, InsightCode

# A spreadsheet cell holding this value is treated the same as an empty cell. See NeatTableReader.read
# for details.
CELL_MISSING = "#N/A"

# A container reference is identified by (space, external_id).
ContainerRef = tuple[str, str]
# A view reference is identified by (space, external_id, version).
ViewRef = tuple[str, str, str]

_VIEW_PROPERTY_ADAPTER: TypeAdapter[Any] = TypeAdapter(ViewPropertyType)
_CONSTRAINT_ADAPTER: TypeAdapter[Any] = TypeAdapter(ConstraintType)
_INDEX_ADAPTER: TypeAdapter[Any] = TypeAdapter(IndexType)


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
    definition: ContainerPropertyDefinition


@dataclass
class _ReadViewProperty:
    prop_id: str
    row_no: int
    definition: Any


@dataclass
class _ReadOrdered:
    """A read index or (uniqueness) constraint entry, which may need to be ordered relative to its
    siblings when it spans multiple properties."""

    prop_id: str
    order: int | None
    row_no: int
    definition: Any


class NeatTableReader:
    """Converts a NEAT table (DMSTableReader equivalent) into Toolkit's YAML resources.

    This is similar to the ``DMSTableReader`` in Neat, see
    https://github.com/cognitedata/neat/blob/main/cognite/neat/_data_model/importers/_table_importer/reader.py,
    but converts to :class:`SpaceYAML`, :class:`ContainerYAML`, :class:`ViewYAML`, and
    :class:`DataModelYAML` instead of Neat's internal request schema.

    Any issue that does not prevent the rest of the table from being converted (an invalid property,
    an unresolved reference, a duplicate entry, and so on) is reported as a warning and the offending
    row/entry is skipped, so the rest of the data model can still be built. Only a handful of truly
    fatal issues (the space or the data model itself cannot be built) raise ``NeatTableError``.

    Locations in warnings/errors point back at the table (sheet), row, and column the issue came from.
    When ``source`` was built from a NEAT YAML file, the row number is the (1-based) position of the
    entry in the YAML list, since there is no spreadsheet row to point to. When ``source`` was built
    from a spreadsheet, the row number is adjusted to the actual Excel row (accounting for the header
    row and any empty rows), see ``NeatTableSource``.
    """

    SHEET_METADATA = "Metadata"
    SHEET_PROPERTIES = "Properties"
    SHEET_VIEWS = "Views"
    SHEET_CONTAINERS = "Containers"
    SHEET_ENUM = "Enum"
    SHEET_NODES = "Nodes"

    def __init__(self, neat_yaml: NeatYAML, source: NeatTableSource):
        self.neat_yaml = neat_yaml
        self.source = source
        self.default_space = neat_yaml.metadata.space
        self.default_version = neat_yaml.metadata.version
        self._warnings: list[BuildWarning] = []
        self._error: BuildError | None = None

    def get_warnings(self) -> list[BuildWarning]:
        return self._warnings

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

    def _warn(self, code: "InsightCode", message: str) -> None:
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning

        self._warnings.append(BuildWarning(code=code, message=message, source_file=Path(self.source.source)))

    def _loc(self, table: str, row: int | None = None, column: str | None = None) -> str:
        path: tuple[int | str, ...]
        if row is None:
            path = (table,)
        elif column is None:
            path = (table, row)
        else:
            path = (table, row, column)
        return self.source.location(path)

    def _warn_invalid(self, error: ValidationError, validation_type: Any, loc: str) -> None:
        for message in humanize_validation_error(error, validation_type):
            self._warn("INVALID-VALUE", f"In {loc}: {message}")

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

    def _parse_entity_str(self, value: str, loc: str) -> ParsedEntity | None:
        try:
            return parse_entity(value)
        except ValueError as e:
            self._warn("INVALID-VALUE", f"In {loc}: failed to parse {value!r}: {e!s}")
            return None

    def _container_refs(self, containers: list[DMSContainer]) -> set[ContainerRef]:
        return {ref for container in containers if (ref := self._container_ref_key(container.container)) is not None}

    def _view_refs(self, views: list[DMSView]) -> set[ViewRef]:
        return {ref for view in views if (ref := self._view_ref_key(view.view)) is not None}

    # --- Space -----------------------------------------------------------------------------------

    def _read_space(self) -> CrudResource[SpaceId]:
        try:
            space_yaml = SpaceYAML.model_validate({"space": self.default_space})
        except ValidationError as e:
            messages = "; ".join(humanize_validation_error(e, SpaceYAML))
            raise NeatTableError(
                f"Invalid space {self.default_space!r} declared in {self._loc(self.SHEET_METADATA)}: {messages}"
            ) from None
        return CrudResource(
            raw=space_yaml.model_dump(by_alias=True, exclude_none=True),
            identifier=space_yaml.as_id(),
            type_=SpaceIO.as_crud_type(),
        )

    # --- Enum ------------------------------------------------------------------------------------

    @staticmethod
    def _read_enum_collections(rows: list[DMSEnum]) -> dict[str, dict[str, dict[str, Any]]]:
        collections: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for row in rows:
            collections[row.collection][row.value] = {"name": row.name, "description": row.description}
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
        dict[ViewRef, dict[str, Any]],
        dict[ContainerRef, dict[str, Any]],
        dict[ContainerRef, dict[str, Any]],
    ]:
        container_reads: dict[tuple[ContainerRef, str], list[_ReadContainerProperty]] = defaultdict(list)
        view_reads: dict[tuple[ViewRef, str], list[_ReadViewProperty]] = defaultdict(list)
        index_reads: dict[tuple[ContainerRef, str], list[_ReadOrdered]] = defaultdict(list)
        constraint_reads: dict[tuple[ContainerRef, str], list[_ReadOrdered]] = defaultdict(list)

        for row_no, prop in enumerate(properties):
            self._process_view_property(prop, row_no, view_reads, view_refs)

            if prop.container is None or prop.container_property is None:
                # This is an edge or reverse direct relation property, which has no container mapping.
                continue

            container_ref = self._container_ref_key(prop.container)
            if container_ref is None:
                self._warn(
                    "INVALID-VALUE",
                    f"In {self._loc(self.SHEET_PROPERTIES, row_no, 'Container')}: the container reference "
                    f"{prop.container!s} is invalid.",
                )
                continue
            if container_ref not in container_refs:
                self._warn(
                    "MISSING-REFERENCED-RESOURCE",
                    f"In {self._loc(self.SHEET_PROPERTIES, row_no, 'Container')}: the container {prop.container!s} "
                    f"was not found in the {self.SHEET_CONTAINERS!r} table.",
                )
                continue

            self._process_container_property(prop, row_no, container_ref, enum_collections, container_reads)
            self._process_ordered(
                prop.index, prop.container_property, row_no, container_ref, "Index", _INDEX_ADAPTER, index_reads
            )
            self._process_ordered(
                prop.constraint,
                prop.container_property,
                row_no,
                container_ref,
                "Constraint",
                _CONSTRAINT_ADAPTER,
                constraint_reads,
            )

        container_props = self._finalize_container_properties(container_reads)
        view_props = self._finalize_view_properties(view_reads)
        indices = self._finalize_ordered(index_reads, "index", lambda _: True)
        constraints = self._finalize_ordered(
            constraint_reads, "constraint", lambda d: isinstance(d, UniquenessConstraintDefinition)
        )
        return container_props, view_props, indices, constraints

    def _process_view_property(
        self,
        prop: DMSProperty,
        row_no: int,
        view_reads: dict[tuple[ViewRef, str], list[_ReadViewProperty]],
        view_refs: set[ViewRef],
    ) -> None:
        loc = self._loc(self.SHEET_PROPERTIES, row_no)
        data = self._build_view_property(prop, row_no)
        if data is None:
            return
        try:
            view_property = _VIEW_PROPERTY_ADAPTER.validate_python(data)
        except ValidationError as e:
            self._warn_invalid(e, ViewPropertyType, loc)
            return

        view_ref = self._view_ref_key(prop.view)
        if view_ref is None:
            self._warn("INVALID-VALUE", f"In {self._loc(self.SHEET_PROPERTIES, row_no, 'View')}: invalid view.")
            return
        if view_ref not in view_refs:
            self._warn(
                "MISSING-REFERENCED-RESOURCE",
                f"In {self._loc(self.SHEET_PROPERTIES, row_no, 'View')}: the view {prop.view!s} was not found "
                f"in the {self.SHEET_VIEWS!r} table.",
            )
            return
        view_reads[(view_ref, prop.view_property)].append(_ReadViewProperty(prop.view_property, row_no, view_property))

    def _build_view_property(self, prop: DMSProperty, row_no: int) -> dict[str, Any] | None:
        if prop.connection is None or prop.connection.suffix == "direct":
            return self._build_core_view_property(prop)
        elif prop.connection.suffix == "edge":
            return self._build_edge_view_property(prop, row_no)
        elif prop.connection.suffix == "reverse":
            return self._build_reverse_view_property(prop, row_no)
        self._warn(
            "INVALID-VALUE",
            f"In {self._loc(self.SHEET_PROPERTIES, row_no, 'Connection')}: invalid connection type "
            f"{prop.connection.suffix!r}.",
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
        loc = self._loc(self.SHEET_PROPERTIES, row_no, "Connection")

        edge_source = None
        if raw_edge_source := prop.connection.properties.get("edgeSource"):
            if (entity := self._parse_entity_str(raw_edge_source, loc)) is not None:
                edge_source = self._view_ref_dict(entity)

        if raw_type := prop.connection.properties.get("type"):
            type_entity = self._parse_entity_str(raw_type, loc)
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

    def _process_container_property(
        self,
        prop: DMSProperty,
        row_no: int,
        container_ref: ContainerRef,
        enum_collections: dict[str, dict[str, dict[str, Any]]],
        container_reads: dict[tuple[ContainerRef, str], list[_ReadContainerProperty]],
    ) -> None:
        assert prop.container_property is not None
        loc = self._loc(self.SHEET_PROPERTIES, row_no)
        data = self._build_container_property(prop, enum_collections, row_no)
        try:
            definition = ContainerPropertyDefinition.model_validate(data)
        except ValidationError as e:
            self._warn_invalid(e, ContainerPropertyDefinition, loc)
            return
        container_reads[(container_ref, prop.container_property)].append(
            _ReadContainerProperty(prop.container_property, row_no, definition)
        )

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
                loc = self._loc(self.SHEET_PROPERTIES, row_no, "Connection")
                entity = self._parse_entity_str(raw_container, loc)
                if entity is not None:
                    args["container"] = self._container_ref_dict(entity)

        if value_type == "enum" and "collection" in prop.value_type.properties:
            # 'collection' is a table-only convention used to look up the Enum sheet; it is not a
            # recognized field on the enum property type itself.
            args["values"] = enum_collections.get(args.pop("collection"), {})
        return args

    def _process_ordered(
        self,
        entities: list[ParsedEntity] | None,
        prop_id: str | None,
        row_no: int,
        container_ref: ContainerRef,
        column: str,
        adapter: TypeAdapter[Any],
        reads: dict[tuple[ContainerRef, str], list[_ReadOrdered]],
    ) -> None:
        if entities is None or prop_id is None:
            return
        key_name = "indexType" if column == "Index" else "constraintType"
        loc = self._loc(self.SHEET_PROPERTIES, row_no, column)
        for entity in entities:
            # 'order' is only used to sort multi-property indices/constraints below; it is not a
            # recognized field on the index/constraint definition itself, so it must not be passed through.
            extra_properties = dict(entity.properties)
            order_raw = extra_properties.pop("order", None)
            data = {key_name: entity.prefix, "properties": [prop_id], **extra_properties}
            try:
                definition = adapter.validate_python(data)
            except ValidationError as e:
                self._warn_invalid(e, adapter, loc)
                continue
            order = self._read_order(order_raw, loc)
            reads[(container_ref, entity.suffix)].append(_ReadOrdered(prop_id, order, row_no, definition))

    def _read_order(self, order_raw: str | None, loc: str) -> int | None:
        if order_raw is None:
            return None
        try:
            return int(order_raw)
        except ValueError:
            self._warn("INVALID-VALUE", f"In {loc}: invalid order value {order_raw!r}, must be an integer.")
            return None

    def _finalize_container_properties(
        self, reads: dict[tuple[ContainerRef, str], list[_ReadContainerProperty]]
    ) -> dict[ContainerRef, dict[str, ContainerPropertyDefinition]]:
        result: dict[ContainerRef, dict[str, ContainerPropertyDefinition]] = defaultdict(dict)
        for (container_ref, prop_id), entries in reads.items():
            result[container_ref][prop_id] = entries[0].definition
            if len(entries) > 1 and any(entry.definition != entries[0].definition for entry in entries[1:]):
                rows = humanize_collection([self._loc(self.SHEET_PROPERTIES, entry.row_no) for entry in entries])
                self._warn(
                    "INVALID-VALUE",
                    f"Multiple properties map to container {container_ref[0]}:{container_ref[1]} property "
                    f"{prop_id!r} with different definitions. Using the first definition found, from rows: {rows}.",
                )
        return result

    def _finalize_view_properties(
        self, reads: dict[tuple[ViewRef, str], list[_ReadViewProperty]]
    ) -> dict[ViewRef, dict[str, Any]]:
        result: dict[ViewRef, dict[str, Any]] = defaultdict(dict)
        for (view_ref, prop_id), entries in reads.items():
            result[view_ref][prop_id] = entries[0].definition
            if len(entries) > 1:
                rows = humanize_collection([self._loc(self.SHEET_PROPERTIES, entry.row_no) for entry in entries])
                self._warn(
                    "INVALID-VALUE",
                    f"The combination of 'View' and 'View Property' must be unique. Duplicated entries for view "
                    f"{view_ref[0]}:{view_ref[1]}(version={view_ref[2]}) property {prop_id!r} found in rows: {rows}.",
                )
        return result

    def _finalize_ordered(
        self,
        reads: dict[tuple[ContainerRef, str], list[_ReadOrdered]],
        kind: str,
        requires_order: Callable[[Any], bool],
    ) -> dict[ContainerRef, dict[str, Any]]:
        result: dict[ContainerRef, dict[str, Any]] = defaultdict(dict)
        for (container_ref, identifier), entries in reads.items():
            # Deduplicate by property identifier, keeping the first occurrence.
            unique_entries = list({entry.prop_id: entry for entry in entries}.values())
            definition = unique_entries[0].definition
            if len(unique_entries) == 1 or not requires_order(definition):
                result[container_ref][identifier] = definition
                continue
            if missing_order := [entry for entry in unique_entries if entry.order is None]:
                rows = humanize_collection([self._loc(self.SHEET_PROPERTIES, entry.row_no) for entry in missing_order])
                self._warn(
                    "INVALID-VALUE",
                    f"The {kind} {identifier!r} on container {container_ref[0]}:{container_ref[1]} is defined on "
                    f"multiple properties and requires the 'order' property to be set. It is missing in rows: {rows}.",
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
        indices: dict[ContainerRef, dict[str, Any]],
        constraints: dict[ContainerRef, dict[str, Any]],
    ) -> list[CrudResource[ContainerId]]:
        results: list[CrudResource[ContainerId]] = []
        rows_by_ref: dict[ContainerRef, list[int]] = defaultdict(list)

        for row_no, container in enumerate(containers):
            ref = self._container_ref_key(container.container)
            if ref is None:
                self._warn(
                    "INVALID-VALUE",
                    f"In {self._loc(self.SHEET_CONTAINERS, row_no, 'Container')}: invalid container reference.",
                )
                continue

            property_constraints = constraints.get(ref, {})
            container_constraints, errors = self._read_container_constraints(container, row_no)
            for message in errors:
                self._warn("INVALID-VALUE", message)
            if conflict := set(property_constraints) & set(container_constraints):
                self._warn(
                    "INVALID-VALUE",
                    f"In {self._loc(self.SHEET_CONTAINERS, row_no, 'Constraint')}: container {ref[0]}:{ref[1]} has "
                    f"constraint identifier(s) that conflict with a uniqueness constraint defined in the "
                    f"{self.SHEET_PROPERTIES!r} table: {humanize_collection(conflict)}.",
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
                self._warn_invalid(e, ContainerYAML, self._loc(self.SHEET_CONTAINERS, row_no))
                continue

            rows_by_ref[ref].append(row_no)
            if len(rows_by_ref[ref]) > 1:
                rows = humanize_collection([self._loc(self.SHEET_CONTAINERS, r) for r in rows_by_ref[ref]])
                self._warn(
                    "INVALID-VALUE",
                    f"The values in column 'Container' must be unique. Duplicated entries for container "
                    f"{ref[0]}:{ref[1]} found in rows: {rows}.",
                )
                continue

            results.append(
                CrudResource(
                    raw=container_yaml.model_dump(by_alias=True, exclude_none=True),
                    identifier=container_yaml.as_id(),
                    type_=ContainerIO.as_crud_type(),
                )
            )
        return results

    def _read_container_constraints(self, container: DMSContainer, row_no: int) -> tuple[dict[str, Any], list[str]]:
        result: dict[str, Any] = {}
        errors: list[str] = []
        if not container.constraint:
            return result, errors
        loc = self._loc(self.SHEET_CONTAINERS, row_no, "Constraint")
        for entity in container.constraint:
            if entity.prefix != "requires":
                errors.append(
                    f"In {loc}: the constraint {entity.suffix!r} has an invalid type {entity.prefix!r}. Only "
                    "'requires' constraints are supported at the container level."
                )
                continue
            if "require" not in entity.properties:
                errors.append(
                    f"In {loc}: the constraint {entity.suffix!r} is missing the 'require' property, which is "
                    "required for container-level constraints."
                )
                continue
            required_entity = self._parse_entity_str(entity.properties["require"], loc)
            data = {
                "constraintType": "requires",
                "require": self._container_ref_dict(required_entity) if required_entity is not None else {},
            }
            try:
                definition = _CONSTRAINT_ADAPTER.validate_python(data)
            except ValidationError as e:
                errors.extend(f"In {loc}: {message}" for message in humanize_validation_error(e, ConstraintType))
                continue
            result[entity.suffix] = definition
        return result, errors

    # --- Views -------------------------------------------------------------------------------------

    def _read_views(
        self, views: list[DMSView], view_props: dict[ViewRef, dict[str, Any]]
    ) -> tuple[list[CrudResource[ViewId]], set[ViewRef]]:
        results: list[CrudResource[ViewId]] = []
        rows_by_ref: dict[ViewRef, list[int]] = defaultdict(list)
        valid_refs: set[ViewRef] = set()

        for row_no, view in enumerate(views):
            ref = self._view_ref_key(view.view)
            if ref is None:
                self._warn(
                    "INVALID-VALUE", f"In {self._loc(self.SHEET_VIEWS, row_no, 'View')}: invalid view reference."
                )
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
                self._warn_invalid(e, ViewYAML, self._loc(self.SHEET_VIEWS, row_no))
                continue

            rows_by_ref[ref].append(row_no)
            if len(rows_by_ref[ref]) > 1:
                rows = humanize_collection([self._loc(self.SHEET_VIEWS, r) for r in rows_by_ref[ref]])
                self._warn(
                    "INVALID-VALUE",
                    f"The values in column 'View' must be unique. Duplicated entries for view "
                    f"{ref[0]}:{ref[1]}(version={ref[2]}) found in rows: {rows}.",
                )
                continue

            valid_refs.add(ref)
            results.append(
                CrudResource(
                    raw=view_yaml.model_dump(by_alias=True, exclude_none=True),
                    identifier=view_yaml.as_id(),
                    type_=ViewIO.as_crud_type(),
                )
            )
        return results, valid_refs

    def _build_filter_dict(self, filter_: TableViewFilter, row_no: int) -> dict[str, Any] | None:
        loc = self._loc(self.SHEET_VIEWS, row_no, "Filter")
        if isinstance(filter_, RAWFilterTableFilter):
            try:
                return json.loads(filter_.filter)
            except ValueError as e:
                self._warn("INVALID-VALUE", f"In {loc}: filter must be valid JSON. Got error: {e!s}")
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
            loc = self._loc(self.SHEET_NODES, row_no, "Node")
            if not node.node.suffix or node.node.suffix == CELL_MISSING:
                self._warn("INVALID-VALUE", f"In {loc}: invalid node reference.")
                continue
            data = {"space": node.node.prefix or self.default_space, "externalId": node.node.suffix}
            try:
                node_yaml = NodeYAML.model_validate(data)
            except ValidationError as e:
                self._warn_invalid(e, NodeYAML, loc)
                continue
            results.append(
                CrudResource(
                    raw=node_yaml.model_dump(by_alias=True, exclude_none=True),
                    identifier=node_yaml.as_id(),
                    type_=NodeIO.as_crud_type(),
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
            messages = "; ".join(humanize_validation_error(e, DataModelYAML))
            raise NeatTableError(
                f"Could not build the data model declared in {self._loc(self.SHEET_METADATA)}: {messages}"
            ) from None
        return CrudResource(
            raw=data_model_yaml.model_dump(by_alias=True, exclude_none=True),
            identifier=data_model_yaml.as_id(),
            type_=DataModelIO.as_crud_type(),
        )
