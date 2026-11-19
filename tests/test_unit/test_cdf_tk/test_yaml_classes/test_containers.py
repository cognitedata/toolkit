from collections.abc import Iterable
from pathlib import Path
from typing import get_args

import pytest
import yaml

from cognite_toolkit._cdf_tk.data_classes._insights import BuildWarning
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import ContainerIO
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.yaml_classes.container_field_definitions import (
    ConstraintDefinition,
    ConstraintType,
    IndexDefinition,
    IndexType,
    PropertyType,
    PropertyTypeDefinition,
)
from cognite_toolkit._cdf_tk.yaml_classes.containers import ContainerYAML
from tests.test_unit.utils import find_resources


def container_property_type_cases() -> Iterable:
    container_property_types = [
        {
            "TextProperty": {
                "space": "my_space",
                "externalId": "my_container",
                "name": "My Container",
                "description": "A test container with text property",
                "properties": {
                    "textField": {
                        "name": "Text Field",
                        "description": "A simple text field",
                        "type": {
                            "type": "text",
                            "collation": "ucs_basic",
                            "maxListSize": 10,
                        },
                    }
                },
            }
        },
        {
            "BooleanProperty": {
                "space": "my_space",
                "externalId": "bool_container",
                "properties": {"isActive": {"type": {"type": "boolean"}}},
            }
        },
        {
            "FloatProperty": {
                "space": "my_space",
                "externalId": "float_container",
                "properties": {
                    "temperature": {
                        "type": {"type": "float32", "unit": {"externalId": "temperature", "sourceUnit": "celsius"}}
                    }
                },
            }
        },
        {
            "DirectRelationProperty": {
                "space": "my_space",
                "externalId": "relation_container",
                "properties": {
                    "connectedTo": {
                        "type": {
                            "type": "direct",
                            "container": {"type": "container", "space": "my_space", "externalId": "target_container"},
                        }
                    }
                },
            }
        },
        {
            "EnumProperty": {
                "space": "my_space",
                "externalId": "enum_container",
                "properties": {
                    "status": {
                        "type": {
                            "type": "enum",
                            "values": {
                                "active": {"name": "Active", "description": "Resource is active"},
                                "inactive": {"name": "Inactive", "description": "Resource is inactive"},
                            },
                        }
                    }
                },
            }
        },
    ]
    yield from (pytest.param(next(iter(pt.values())), id=next(iter(pt.keys()))) for pt in container_property_types)


def container_with_constraints_cases() -> Iterable:
    container_constraints = [
        {
            "UniquenessConstraint": {
                "space": "my_space",
                "externalId": "unique_container",
                "properties": {"identifier": {"type": {"type": "text"}}},
                "constraints": {
                    "uniqueIdentifier": {"constraintType": "uniqueness", "properties": ["identifier"], "bySpace": True}
                },
            }
        },
        {
            "RequiresConstraint": {
                "space": "my_space",
                "externalId": "requires_container",
                "properties": {"name": {"type": {"type": "text"}}},
                "constraints": {
                    "requiresOtherContainer": {
                        "constraintType": "requires",
                        "require": {"type": "container", "space": "my_space", "externalId": "dependency_container"},
                    }
                },
            }
        },
    ]
    yield from (pytest.param(next(iter(c.values())), id=next(iter(c.keys()))) for c in container_constraints)


def container_with_indexes_cases() -> Iterable:
    container_indexes = [
        {
            "BtreeIndex": {
                "space": "my_space",
                "externalId": "indexed_container",
                "properties": {"timestamp": {"type": {"type": "timestamp"}}, "value": {"type": {"type": "float64"}}},
                "indexes": {
                    "timeSeriesIndex": {
                        "indexType": "btree",
                        "properties": ["timestamp", "value"],
                        "bySpace": True,
                        "cursorable": True,
                    }
                },
            }
        }
    ]
    yield from (pytest.param(next(iter(idx.values())), id=next(iter(idx.keys()))) for idx in container_indexes)


def invalid_container_cases() -> Iterable:
    yield pytest.param(
        {
            "space": "my_space",
            "externalId": "Query",
            "properties": {"field": {"type": {"type": "text"}}},
        },
        "is a reserved container External ID",
        id="ReservedExternalId",
    )
    yield pytest.param(
        {
            "space": "my_space",
            "externalId": "valid_container",
            "properties": {"invalid@": {"type": {"type": "text"}}},
        },
        "does not match the required pattern",
        id="InvalidPropertyIdentifier",
    )
    yield pytest.param(
        {
            "space": "my_space",
            "externalId": "valid_container",
            "properties": {"edge_id": {"type": {"type": "text"}}},
        },
        "is a reserved property identifier",
        id="ForbiddenPropertyIdentifier",
    )
    yield pytest.param(
        {
            "space": "invalid space",  # Contains a space
            "externalId": "valid_container",
            "properties": {"field": {"type": {"type": "text"}}},
        },
        ".*",
        id="InvalidSpaceFormat",
    )
    yield pytest.param(
        {
            "space": "my_space",
            "externalId": "record_container",
            "usedFor": "record",
            "properties": {"field": {"type": {"type": "text"}}},
            "indexes": {
                "recordIndex": {
                    "indexType": "btree",
                    "properties": ["field"],
                    "bySpace": True,
                    "cursorable": True,
                }
            },
        },
        "Indexes and constraints are not supported for record containers",
        id="InvalidRecordContainer",
    )
    yield pytest.param(
        {
            "space": "my_space",
            "externalId": "record_container",
            "usedFor": "record",
            "properties": {"field": {"type": {"type": "timeseries"}}},
        },
        "External CDF references are not supported for record containers.",
        id="InvalidRecordContainer",
    )
    yield pytest.param(
        {
            "space": "my_space",
            "externalId": "my_container",
            "properties": {"pointer": {"nullable": False, "type": {"type": "direct"}}},
        },
        "Direct relations must be nullable",
        id="NonNullableDirectRelation",
    )


class TestContainerYAML:
    @pytest.mark.parametrize("data", list(find_resources("Container")))
    def test_load_valid_container_file(self, data: dict[str, object]) -> None:
        loaded = ContainerYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "data",
        [
            *container_property_type_cases(),
            *container_with_constraints_cases(),
            *container_with_indexes_cases(),
        ],
    )
    def test_load_valid_container_cases(self, data: dict[str, object]) -> None:
        loaded = ContainerYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data, expected_match", list(invalid_container_cases()))
    def test_invalid_container(self, data: dict[str, object], expected_match: str) -> None:
        with pytest.raises(ValueError, match=expected_match):
            ContainerYAML.model_validate(data)

    def test_all_property_types_in_union(self) -> None:
        """Test that all property types are included in the union."""
        expected_subclasses = set(get_concrete_subclasses(PropertyTypeDefinition))
        subclasses = set(get_args(PropertyType.__args__[0]))

        assert subclasses == expected_subclasses, f"Expected subclasses {expected_subclasses}, but got {subclasses}"

    def test_all_constraints_in_union(self) -> None:
        """Test that all constraint types are included in the union."""
        expected_subclasses = set(get_concrete_subclasses(ConstraintDefinition))
        subclasses = set(get_args(ConstraintType.__args__[0]))

        assert subclasses == expected_subclasses, f"Expected subclasses {expected_subclasses}, but got {subclasses}"

    def test_all_indexes_in_union(self) -> None:
        """Test that all index types are included in the union."""
        expected_subclasses = set(get_concrete_subclasses(IndexDefinition))
        subclasses = set(get_args(IndexType.__args__[0]))

        assert subclasses == expected_subclasses, f"Expected subclasses {expected_subclasses}, but got {subclasses}"

    def test_deprecated_cdf_reference_warnings(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.ALPHA_RULES)
        source = tmp_path / "my.Container.yaml"
        source.write_text(
            "space: my_space\n"
            "externalId: my_container\n"
            "properties:\n"
            "  name:\n"
            "    type:\n"
            "      type: text\n"
            "  ts:\n"
            "    type:\n"
            "      type: timeseries\n"
            "  doc:\n"
            "    type:\n"
            "      type: file\n"
            "  table:\n"
            "    type:\n"
            "      type: sequence\n"
        )
        container = ContainerYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_container",
                "properties": {
                    "name": {"type": {"type": "text"}},
                    "ts": {"type": {"type": "timeseries"}},
                    "doc": {"type": {"type": "file"}},
                    "table": {"type": {"type": "sequence"}},
                },
            }
        )

        insights = container.extra_insights(source, ContainerIO.as_resource_type())

        assert [(insight.code, insight.line) for insight in insights] == [
            ("DEPRECATED-FEATURE", 7),
            ("DEPRECATED-FEATURE", 10),
            ("DEPRECATED-FEATURE", 13),
        ]

    @pytest.mark.parametrize(
        "type_by_property, index_type, expected",
        [
            pytest.param(
                {"code": "{type: text}"},
                "btree",
                [(BuildWarning, "MISSING-VALUE", "has no maxTextSize")],
                id="text-without-max-size",
            ),
            pytest.param({"code": "{type: text, maxTextSize: 255}"}, "btree", [], id="bounded-text"),
            pytest.param(
                {"code": "{type: text, maxTextSize: 4000}"},
                "btree",
                [(BuildWarning, "EXCEEDED-LIMIT", "up to 4000 bytes")],
                id="text-too-large",
            ),
            pytest.param(
                {"codes": "{type: int64, list: true}"},
                "btree",
                [(BuildWarning, "MISSING-VALUE", "has no maxListSize")],
                id="list-without-max-size",
            ),
            pytest.param(
                {"codes": "{type: text, list: true}"},
                "btree",
                [
                    (BuildWarning, "MISSING-VALUE", "has no maxListSize"),
                    (BuildWarning, "MISSING-VALUE", "has no maxTextSize"),
                ],
                id="text-list-without-bounds",
            ),
            pytest.param({"codes": "{type: int64, list: true, maxListSize: 300}"}, "btree", [], id="int64-at-limit"),
            pytest.param(
                {"codes": "{type: int64, list: true, maxListSize: 301}"},
                "btree",
                [(BuildWarning, "EXCEEDED-LIMIT", "up to 2408 bytes")],
                id="int64-over-limit",
            ),
            pytest.param({"codes": "{type: int32, list: true, maxListSize: 600}"}, "btree", [], id="int32-at-limit"),
            pytest.param(
                {"codes": "{type: float32, list: true, maxListSize: 600}"}, "btree", [], id="float32-at-limit"
            ),
            pytest.param(
                {"codes": "{type: timestamp, list: true, maxListSize: 301}"},
                "btree",
                [(BuildWarning, "EXCEEDED-LIMIT", "up to 2408 bytes")],
                id="timestamp-over-limit",
            ),
            pytest.param(
                {"code": "{type: text, maxTextSize: 1500}", "other": "{type: text, maxTextSize: 1500}"},
                "btree",
                [(BuildWarning, "EXCEEDED-LIMIT", "up to 3000 bytes")],
                id="composite-too-large",
            ),
            pytest.param({"codes": "{type: text, list: true}"}, "inverted", [], id="inverted-index"),
        ],
    )
    def test_btree_index_size_insights(
        self,
        type_by_property: dict[str, str],
        index_type: str,
        expected: list[tuple[type, str, str]],
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.ALPHA_RULES)
        properties = "".join(
            f"  {prop_id}:\n    type: {prop_type}\n" for prop_id, prop_type in type_by_property.items()
        )
        index_properties = "".join(f"    - {prop_id}\n" for prop_id in type_by_property)
        source = tmp_path / "my.Container.yaml"
        source.write_text(
            f"space: my_space\nexternalId: my_container\nproperties:\n{properties}"
            f"indexes:\n  idx:\n    indexType: {index_type}\n    properties:\n{index_properties}"
        )
        container = ContainerYAML.model_validate(yaml.safe_load(source.read_text()))

        insights = container.extra_insights(source, ContainerIO.as_resource_type())
        assert [
            (type(insight), insight.code, fragment in insight.message)
            for insight, (_, _, fragment) in zip(insights, expected, strict=True)
        ] == [(insight_type, code, True) for insight_type, code, _ in expected]
