from collections.abc import Iterable

import pytest

from cognite_toolkit._cdf_tk.resource_classes.containers import ContainerYAML
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


class TestContainerYAML:
    @pytest.mark.parametrize("data", list(find_resources("Container")))
    def test_load_valid_container_file(self, data: dict[str, object]) -> None:
        loaded = ContainerYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", container_property_type_cases())
    def test_load_valid_container_property_types(self, data: dict[str, object]) -> None:
        loaded = ContainerYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", container_with_constraints_cases())
    def test_load_valid_container_constraints(self, data: dict[str, object]) -> None:
        loaded = ContainerYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", container_with_indexes_cases())
    def test_load_valid_container_indexes(self, data: dict[str, object]) -> None:
        loaded = ContainerYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_invalid_external_id(self) -> None:
        invalid_data = {
            "space": "my_space",
            "externalId": "Query",
            "properties": {"field": {"type": {"type": "text"}}},
        }

        with pytest.raises(ValueError, match="is a reserved container External ID"):
            ContainerYAML.model_validate(invalid_data)

    def test_invalid_property_identifier(self) -> None:
        invalid_data = {
            "space": "my_space",
            "externalId": "valid_container",
            "properties": {"invalid@": {"type": {"type": "text"}}},
        }

        with pytest.raises(ValueError, match="does not match the required pattern"):
            ContainerYAML.model_validate(invalid_data)

    def test_forbidden_property_identifier(self) -> None:
        invalid_data = {
            "space": "my_space",
            "externalId": "valid_container",
            "properties": {"edge_id": {"type": {"type": "text"}}},
        }

        with pytest.raises(ValueError, match="is a reserved property identifier"):
            ContainerYAML.model_validate(invalid_data)

    def test_space_format_validation(self) -> None:
        invalid_data = {
            "space": "invalid space",  # Contains a space
            "externalId": "valid_container",
            "properties": {"field": {"type": {"type": "text"}}},
        }

        with pytest.raises(ValueError):
            ContainerYAML.model_validate(invalid_data)
