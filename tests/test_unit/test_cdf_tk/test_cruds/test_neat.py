import copy
from contextlib import suppress
from pathlib import Path
from typing import Any

import pytest
import yaml

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, NodeId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError, BuildWarning
from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.resource_ios._neat._neat_io import NeatIO
from cognite_toolkit._cdf_tk.resource_ios._neat._reader import NeatTableError, NeatTableReader
from cognite_toolkit._cdf_tk.yaml_classes.neat import NeatYAML
from cognite_toolkit._cdf_tk.yaml_classes.neat._table_source import NeatSpreadsheetSource

VALID_NEAT_TABLE: dict[str, Any] = {
    "Metadata": {"space": "my_space", "external_id": "MyModel", "version": "v1", "name": "My Model"},
    "Properties": [
        {
            "View": "Asset",
            "View Property": "name",
            "Value Type": "text",
            "Container": "Asset",
            "Container Property": "name",
            "Index": "btree:nameIdx",
            "Constraint": "uniqueness:nameUnique",
        },
        {
            "View": "Asset",
            "View Property": "status",
            "Value Type": "enum(collection=Status)",
            "Container": "Asset",
            "Container Property": "status",
        },
        {
            "View": "Asset",
            "View Property": "equipment",
            "Connection": "reverse(property=asset)",
            "Value Type": "Equipment",
            "Max Count": None,
        },
        {
            "View": "Equipment",
            "View Property": "asset",
            "Connection": "direct(container=Asset)",
            "Value Type": "Asset",
            "Container": "Equipment",
            "Container Property": "asset",
        },
        {
            "View": "Equipment",
            "View Property": "serialNumber",
            "Value Type": "text",
            "Min Count": 1,
            "Container": "Equipment",
            "Container Property": "serialNumber",
            "Index": "btree:serialIdx(order=1)",
        },
        {
            "View": "Equipment",
            "View Property": "code",
            "Value Type": "int32",
            "Container": "Equipment",
            "Container Property": "code",
            "Index": "btree:serialIdx(order=2)",
        },
        {
            "View": "Equipment",
            "View Property": "related",
            "Connection": "edge(type=relatedTo)",
            "Value Type": "Equipment",
            "Max Count": None,
        },
    ],
    "Views": [
        {"View": "Asset", "Name": "Asset", "Filter": "hasData(Asset)"},
        {"View": "Equipment", "Implements": "Asset", "Filter": "nodeType(EquipmentType)"},
    ],
    "Containers": [
        {"Container": "Asset", "Used For": "node"},
        {"Container": "Equipment", "Constraint": "requires:assetRequired(require=Asset)"},
    ],
    "Enum": [
        {"Collection": "Status", "Value": "active", "Name": "Active"},
        {"Collection": "Status", "Value": "inactive"},
    ],
    "Nodes": [{"Node": "EquipmentType"}],
}

ASSET_CONTAINER = {"type": "container", "space": "my_space", "externalId": "Asset"}
EQUIPMENT_CONTAINER = {"type": "container", "space": "my_space", "externalId": "Equipment"}
ASSET_VIEW = {"type": "view", "space": "my_space", "externalId": "Asset", "version": "v1"}
EQUIPMENT_VIEW = {"type": "view", "space": "my_space", "externalId": "Equipment", "version": "v1"}


def _new_property(**overrides: Any) -> dict[str, Any]:
    """A new, valid property row on the Equipment view, mapped to the Equipment container."""
    return {
        "View": "Equipment",
        "View Property": "other",
        "Value Type": "text",
        "Container": "Equipment",
        "Container Property": "other",
        **overrides,
    }


def _new_edge(connection: str) -> dict[str, Any]:
    return {"View": "Equipment", "View Property": "other", "Connection": connection, "Value Type": "Equipment"}


def _create_reader(data: dict[str, Any]) -> NeatTableReader:
    source = NeatSpreadsheetSource(source=Path("model.xlsx").absolute(), table_read={})
    return NeatTableReader(NeatYAML.model_validate(data), source)


class TestNeatTableReader:
    def test_read_valid_table(self) -> None:
        reader = _create_reader(VALID_NEAT_TABLE)

        result = reader.read()

        assert reader.get_errors() == []
        assert reader.get_warnings() == []
        assert result.space.identifier == SpaceId(space="my_space")
        assert {container.identifier: container.raw for container in result.containers} == {
            ContainerId(space="my_space", external_id="Asset"): {
                "space": "my_space",
                "externalId": "Asset",
                "usedFor": "node",
                "properties": {
                    "name": {"nullable": True, "type": {"type": "text", "list": False}},
                    "status": {
                        "nullable": True,
                        "type": {"type": "enum", "values": {"active": {"name": "Active"}, "inactive": {}}},
                    },
                },
                "constraints": {"nameUnique": {"constraintType": "uniqueness", "properties": ["name"]}},
                "indexes": {"nameIdx": {"indexType": "btree", "properties": ["name"]}},
            },
            ContainerId(space="my_space", external_id="Equipment"): {
                "space": "my_space",
                "externalId": "Equipment",
                "properties": {
                    "asset": {
                        "nullable": True,
                        "type": {"type": "direct", "list": False, "container": ASSET_CONTAINER},
                    },
                    "serialNumber": {"nullable": False, "type": {"type": "text", "list": False}},
                    "code": {"nullable": True, "type": {"type": "int32", "list": False}},
                },
                "constraints": {"assetRequired": {"constraintType": "requires", "require": ASSET_CONTAINER}},
                "indexes": {"serialIdx": {"indexType": "btree", "properties": ["serialNumber", "code"]}},
            },
        }
        assert {view.identifier: view.raw for view in result.views} == {
            ViewId(space="my_space", external_id="Asset", version="v1"): {
                "space": "my_space",
                "externalId": "Asset",
                "version": "v1",
                "name": "Asset",
                "filter": {"hasData": [ASSET_CONTAINER]},
                "properties": {
                    "name": {"container": ASSET_CONTAINER, "containerPropertyIdentifier": "name"},
                    "status": {"container": ASSET_CONTAINER, "containerPropertyIdentifier": "status"},
                    "equipment": {
                        "connectionType": "multi_reverse_direct_relation",
                        "source": EQUIPMENT_VIEW,
                        "through": {"source": EQUIPMENT_VIEW, "identifier": "asset"},
                    },
                },
            },
            ViewId(space="my_space", external_id="Equipment", version="v1"): {
                "space": "my_space",
                "externalId": "Equipment",
                "version": "v1",
                "filter": {
                    "equals": {
                        "property": ["node", "type"],
                        "value": {"space": "my_space", "externalId": "EquipmentType"},
                    }
                },
                "implements": [ASSET_VIEW],
                "properties": {
                    "asset": {
                        "container": EQUIPMENT_CONTAINER,
                        "containerPropertyIdentifier": "asset",
                        "source": ASSET_VIEW,
                    },
                    "serialNumber": {"container": EQUIPMENT_CONTAINER, "containerPropertyIdentifier": "serialNumber"},
                    "code": {"container": EQUIPMENT_CONTAINER, "containerPropertyIdentifier": "code"},
                    "related": {
                        "connectionType": "multi_edge_connection",
                        "source": EQUIPMENT_VIEW,
                        "type": {"space": "my_space", "externalId": "relatedTo"},
                        "direction": "outwards",
                    },
                },
            },
        }
        assert {node.identifier: node.raw for node in result.node_types} == {
            NodeId(space="my_space", external_id="EquipmentType"): {"space": "my_space", "externalId": "EquipmentType"}
        }
        assert result.data_model.identifier == DataModelId(space="my_space", external_id="MyModel", version="v1")
        assert result.data_model.raw == {
            "space": "my_space",
            "externalId": "MyModel",
            "version": "v1",
            "name": "My Model",
            "views": [ASSET_VIEW, EQUIPMENT_VIEW],
        }

    @pytest.mark.parametrize(
        "changes, expected",
        [
            pytest.param(
                {"Metadata": {"space": "space"}},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Metadata': Invalid value for space: 'space' is a reserved space. "
                        "Reserved Spaces: cdf, dms, edge, node, pg3, shared, space and system",
                    )
                ],
                id="Invalid space",
            ),
            pytest.param(
                {"Properties": [_new_property(**{"Connection": "direct(container=a:b:c)", "Value Type": "Asset"})]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties' row 8 column 'Connection': Failed to parse 'a:b:c': "
                        "Unexpected characters after properties at position 3. Got ':'",
                    )
                ],
                id="Unparsable entity in connection",
            ),
            pytest.param(
                {"Properties": [_new_edge("edge(direction=sideways)")]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties' row 8: Unrecognized value for direction: "
                        "Expected one of 'outwards' or 'inwards'. Got 'sideways'.",
                    )
                ],
                id="Invalid view property",
            ),
            pytest.param(
                {"Properties": [_new_property(View="#N/A")]},
                [(BuildError, "INVALID-VALUE", "table 'Properties' row 8 column 'View': Invalid view reference")],
                id="Invalid view reference in Properties",
            ),
            pytest.param(
                {"Properties": [_new_property(View="Unknown")]},
                [
                    (
                        BuildError,
                        "MISSING-REFERENCED-RESOURCE",
                        "table 'Properties' row 8 column 'View': the view Unknown was not found in the 'Views' table.",
                    )
                ],
                id="View not found in Views table",
            ),
            pytest.param(
                {"Properties": [_new_edge("sideways")]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties' row 8 column 'Connection': invalid connection type 'sideways'",
                    )
                ],
                id="Invalid connection type",
            ),
            pytest.param(
                {"Properties": [_new_property(**{"Value Type": "notAType"})]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties' row 8: Invalid value for type: Input tag 'notAType' found using 'type' "
                        "does not match any of the expected tags: 'text', 'float32', 'float64', 'boolean', 'int32', "
                        "'int64', 'timestamp', 'date', 'json', 'timeseries', 'file', 'sequence', 'direct', 'enum'",
                    )
                ],
                id="Invalid container property",
            ),
            pytest.param(
                {"Properties": [_new_property(Container="#N/A")]},
                [
                    # The invalid container reference makes both the view and container property invalid.
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties' row 8: Missing required fields in container: 'externalId' and 'space'",
                    ),
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties' row 8 column 'Container': the container reference #N/A is invalid.",
                    ),
                ],
                id="Invalid container reference in Properties",
            ),
            pytest.param(
                {"Properties": [_new_property(Container="Unknown")]},
                [
                    (
                        BuildError,
                        "MISSING-REFERENCED-RESOURCE",
                        "table 'Properties' row 8 column 'Container': the container Unknown was not found "
                        "in the 'Containers' table.",
                    )
                ],
                id="Container in governed space not found in Containers table",
            ),
            pytest.param(
                {"Properties": [_new_property(Index="notAType:otherIdx")]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties' row 8 column 'Index': Input tag 'notAType' found using "
                        "'index_type' | 'indexType' does not match any of the expected tags: 'btree', 'inverted'",
                    )
                ],
                id="Invalid index",
            ),
            pytest.param(
                {"Properties": [_new_property(Index="btree:otherIdx(order=first)")]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties' row 8 column 'Index': invalid order value 'first', must be an integer.",
                    )
                ],
                id="Invalid index order",
            ),
            pytest.param(
                {
                    "Properties": [
                        _new_property(
                            **{
                                "View Property": "assetName",
                                "Value Type": "int32",
                                "Container": "Asset",
                                "Container Property": "name",
                            }
                        )
                    ]
                },
                [
                    (
                        BuildWarning,
                        "INVALID-VALUE",
                        "table 'Properties': Multiple properties map to container my_space:Asset property 'name' "
                        "with different definitions. Using the first definition found, from rows: 1 and 8.",
                    )
                ],
                id="Conflicting container property definitions",
            ),
            pytest.param(
                {
                    "Properties": [
                        _new_property(**{"View Property": "code", "Value Type": "int32", "Container Property": "code"})
                    ]
                },
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Properties': The combination of 'View' and 'View Property' must be unique. "
                        "Duplicated entries for view my_space:Equipment(version=v1) property 'code' found in rows: "
                        "6 and 8.",
                    )
                ],
                id="Duplicated view property",
            ),
            pytest.param(
                {"Properties": [_new_property(Index="btree:serialIdx")]},
                [
                    (
                        BuildWarning,
                        "INVALID-VALUE",
                        "table 'Properties': The index 'serialIdx' on container my_space:Equipment is defined on "
                        "multiple properties and requires the 'order' property to be set. It is missing in rows: 8.",
                    )
                ],
                id="Missing order in multi-property index",
            ),
            pytest.param(
                {"Containers": [{"Container": "#N/A"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Containers' row 3 column 'Container': Invalid container reference.",
                    )
                ],
                id="Invalid container reference in Containers",
            ),
            pytest.param(
                {
                    "Containers": [{"Container": "Other", "Constraint": "requires:otherUnique(require=Asset)"}],
                    "Properties": [_new_property(Container="Other", Constraint="uniqueness:otherUnique")],
                },
                [
                    (
                        BuildWarning,
                        "INVALID-VALUE",
                        "table 'Containers' row 3 column 'Constraint': container my_space:Other has constraint "
                        "identifier(s) that conflict with a uniqueness constraint defined in the 'Properties' "
                        "table: otherUnique.",
                    )
                ],
                id="Container constraint conflicts with property constraint",
            ),
            pytest.param(
                {"Containers": [{"Container": "Other", "Used For": "nothing"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Containers' row 3: Unrecognized value for usedFor: "
                        "Expected one of 'node', 'edge', 'record' or 'all'. Got 'nothing'.",
                    )
                ],
                id="Invalid container",
            ),
            pytest.param(
                {"Containers": [{"Container": "Asset"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Containers': The values in column 'Container' must be unique. "
                        "Duplicated entries for container my_space:Asset found in rows: 1 and 3.",
                    )
                ],
                id="Duplicated container",
            ),
            pytest.param(
                {"Containers": [{"Container": "Other", "Constraint": "uniqueness:otherUnique"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Containers' row 3 column 'Constraint': The constraint 'otherUnique' has an invalid "
                        "type 'uniqueness'. Only 'requires' constraints are supported at the container level.",
                    )
                ],
                id="Non-requires constraint in Containers",
            ),
            pytest.param(
                {"Containers": [{"Container": "Other", "Constraint": "requires:otherRequired"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Containers' row 3 column 'Constraint': The constraint 'otherRequired' is missing the "
                        "'require' property, which is required for container-level constraints.",
                    )
                ],
                id="Requires constraint missing require",
            ),
            pytest.param(
                {"Containers": [{"Container": "Other", "Constraint": "requires:otherRequired(require=#N/A)"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Containers' row 3 column 'Constraint': "
                        "Missing required fields in require: 'externalId' and 'space'",
                    )
                ],
                id="Invalid requires constraint",
            ),
            pytest.param(
                {"Views": [{"View": "#N/A"}]},
                [(BuildError, "INVALID-VALUE", "table 'Views' row 3 column 'View': invalid view reference.")],
                id="Invalid view reference in Views",
            ),
            pytest.param(
                {"Views": [{"View": "Other", "Filter": "rawFilter({invalid)"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Views' row 3 column 'Filter': filter must be valid JSON. Got error: "
                        "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
                    )
                ],
                id="Invalid raw filter",
            ),
            pytest.param(
                {"Views": [{"View": "Other", "Implements": "#N/A"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Views' row 3: Missing required fields in implements[1]: "
                        "'externalId', 'space' and 'version'",
                    )
                ],
                id="Invalid view",
            ),
            pytest.param(
                {"Views": [{"View": "Asset"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Views' row 3 column 'View': The values in column 'View' must be unique. "
                        "Duplicated entries for view my_space:Asset(version=v1) found in rows: 1 and 3.",
                    )
                ],
                id="Duplicated view",
            ),
            pytest.param(
                {"Nodes": [{"Node": "#N/A"}]},
                [(BuildError, "INVALID-VALUE", "table 'Nodes' row 2 column 'Node': invalid node reference.")],
                id="Invalid node reference",
            ),
            pytest.param(
                {"Nodes": [{"Node": "invalid space!:MyNode"}]},
                [
                    (
                        BuildError,
                        "INVALID-VALUE",
                        "table 'Nodes' row 2 column 'Node': Invalid value for space: "
                        "String should match pattern '^[a-zA-Z][a-zA-Z0-9_-]{0,41}[a-zA-Z0-9]?$'",
                    )
                ],
                id="Invalid node",
            ),
        ],
    )
    def test_read_table_with_error(
        self, changes: dict[str, Any], expected: list[tuple[type[BuildError | BuildWarning], str, str]]
    ) -> None:
        data = copy.deepcopy(VALID_NEAT_TABLE)
        for table, change in changes.items():
            if table == "Metadata":
                data[table].update(change)
            else:
                data[table].extend(change)
        reader = _create_reader(data)

        # Some errors, such as an invalid space, make it impossible to produce any output.
        with suppress(NeatTableError):
            reader.read()

        insights = [*reader.get_errors(), *reader.get_warnings()]
        assert [(type(insight), insight.code, insight.message) for insight in insights] == expected


class TestNeatIO:
    @staticmethod
    def _write_neat_yaml(tmp_path: Path, data: dict[str, Any]) -> tuple[Path, NeatYAML]:
        source_file = tmp_path / "my_model.Neat.yaml"
        source_file.write_text(yaml.safe_dump(data, sort_keys=False), encoding="utf-8")
        return source_file, NeatYAML.model_validate(data)

    def test_to_crud_types(self, tmp_path: Path) -> None:
        data = copy.deepcopy(VALID_NEAT_TABLE)
        # Containers outside the governed spaces are not returned.
        data["Containers"].append({"Container": "other_space:External"})
        data["Nodes"].append({"Node": "#N/A"})
        source_file, resource = self._write_neat_yaml(tmp_path, data)

        results = list(NeatIO.to_crud_types(data, source_file, resource))

        errors = [result for result in results if isinstance(result, BuildError)]
        assert [(error.message, error.line) for error in errors] == [("invalid node reference.", 69)]
        assert [result.identifier for result in results if isinstance(result, CrudResource)] == [
            SpaceId(space="my_space"),
            ContainerId(space="my_space", external_id="Asset"),
            ContainerId(space="my_space", external_id="Equipment"),
            ViewId(space="my_space", external_id="Asset", version="v1"),
            ViewId(space="my_space", external_id="Equipment", version="v1"),
            NodeId(space="my_space", external_id="EquipmentType"),
            DataModelId(space="my_space", external_id="MyModel", version="v1"),
        ]
        crud_resources = [result for result in results if isinstance(result, CrudResource)]
        assert all(
            result.validated is not None and result.validated.as_id() == result.identifier for result in crud_resources
        )

    def test_to_crud_types_fatal_error(self, tmp_path: Path) -> None:
        data = copy.deepcopy(VALID_NEAT_TABLE)
        data["Metadata"]["space"] = "space"
        source_file, resource = self._write_neat_yaml(tmp_path, data)

        results = list(NeatIO.to_crud_types(data, source_file, resource))

        assert all(isinstance(result, BuildError) for result in results)
        assert [result.message for result in results if isinstance(result, BuildError)] == [
            "Invalid value for space: 'space' is a reserved space. Reserved Spaces: cdf, dms, edge, node, pg3, "
            "shared, space and system",
        ]

    def test_to_crud_types_without_resource(self, tmp_path: Path) -> None:
        # Syntax errors are reported during validation, so no extra error is returned here.
        results = list(NeatIO.to_crud_types({}, tmp_path / "my_model.Neat.yaml", None))

        assert results == []
