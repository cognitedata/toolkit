from __future__ import annotations

import pytest
from cognite.client.data_classes.data_modeling import ContainerApply, SpaceApply

from cognite_toolkit._cdf_tk._parameters import (
    ANY_INT,
    ANY_STR,
    ANYTHING,
    ParameterSet,
    ParameterSpec,
    ParameterSpecSet,
    ParameterValue,
    read_parameter_from_init_type_hints,
    read_parameters_from_dict,
)
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_LIST, ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.location import LocationFilterCRUD


class TestReadParameterFromTypeHints:
    @pytest.mark.parametrize(
        "cls_, expected_parameters",
        [
            (
                SpaceApply,
                ParameterSpecSet(
                    {
                        ParameterSpec(("space",), frozenset({"str"}), True, False),
                        ParameterSpec(("description",), frozenset({"str"}), False, True),
                        ParameterSpec(("name",), frozenset({"str"}), False, True),
                    }
                ),
            ),
            (
                ContainerApply,
                ParameterSpecSet(
                    {
                        ParameterSpec(("space",), frozenset({"str"}), True, False),
                        ParameterSpec(("external_id",), frozenset({"str"}), True, False),
                        ParameterSpec(("properties",), frozenset({"dict"}), True, False),
                        ParameterSpec(
                            path=("properties", ANY_STR),
                            types=frozenset({"dict"}),
                            is_required=False,
                            _is_nullable=False,
                        ),
                        ParameterSpec(("properties", ANY_STR, "type"), frozenset({"dict"}), True, False),
                        ParameterSpec(("properties", ANY_STR, "nullable"), frozenset({"bool"}), False, False),
                        ParameterSpec(("properties", ANY_STR, "auto_increment"), frozenset({"bool"}), False, False),
                        ParameterSpec(("properties", ANY_STR, "immutable"), frozenset({"bool"}), False, False),
                        ParameterSpec(("properties", ANY_STR, "name"), frozenset({"str"}), False, True),
                        ParameterSpec(
                            ("properties", ANY_STR, "default_value"),
                            frozenset({"float", "bool", "str", "int", "dict"}),
                            False,
                            True,
                        ),
                        ParameterSpec(
                            ("properties", ANY_STR, "default_value", ANYTHING), frozenset({"unknown"}), False, True
                        ),
                        ParameterSpec(("properties", ANY_STR, "description"), frozenset({"str"}), False, True),
                        ParameterSpec(("properties", ANY_STR, "type", "collation"), frozenset({"str"}), False, False),
                        ParameterSpec(("properties", ANY_STR, "type", "container"), frozenset({"dict"}), False, True),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "container", "space"), frozenset({"str"}), True, False
                        ),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "container", "external_id"), frozenset({"str"}), True, False
                        ),
                        ParameterSpec(("properties", ANY_STR, "type", "is_list"), frozenset({"bool"}), False, False),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "max_list_size"), frozenset({"int"}), False, True
                        ),
                        ParameterSpec(
                            path=("properties", ANY_STR, "type", "max_text_size"),
                            types=frozenset({"int"}),
                            is_required=False,
                            _is_nullable=True,
                        ),
                        ParameterSpec(("properties", ANY_STR, "type", "unit"), frozenset({"dict"}), False, True),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "unit", "external_id"), frozenset({"str"}), True, False
                        ),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "unit", "source_unit"), frozenset({"str"}), False, True
                        ),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "unknown_value"), frozenset({"str"}), False, True
                        ),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "values"),
                            frozenset({"dict"}),
                            True,
                            False,
                        ),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "values", ANY_STR), frozenset({"dict"}), False, False
                        ),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "values", ANY_STR, "description"),
                            frozenset({"str"}),
                            False,
                            True,
                        ),
                        ParameterSpec(
                            ("properties", ANY_STR, "type", "values", ANY_STR, "name"), frozenset({"str"}), True, False
                        ),
                        ParameterSpec(("description",), frozenset({"str"}), False, True),
                        ParameterSpec(("name",), frozenset({"str"}), False, True),
                        ParameterSpec(("used_for",), frozenset({"str"}), False, True),
                        ParameterSpec(("constraints",), frozenset({"dict"}), False, True),
                        ParameterSpec(
                            path=("constraints", ANY_STR),
                            types=frozenset({"dict"}),
                            is_required=False,
                            _is_nullable=False,
                        ),
                        ParameterSpec(
                            (
                                "constraints",
                                ANY_STR,
                                "require",
                            ),
                            frozenset({"dict"}),
                            True,
                            False,
                        ),
                        ParameterSpec(("constraints", ANY_STR, "require", "space"), frozenset({"str"}), True, False),
                        ParameterSpec(
                            ("constraints", ANY_STR, "require", "external_id"), frozenset({"str"}), True, False
                        ),
                        ParameterSpec(("constraints", ANY_STR, "properties"), frozenset({"list"}), True, False),
                        ParameterSpec(
                            ("constraints", ANY_STR, "properties", ANY_INT), frozenset({"str"}), False, False
                        ),
                        ParameterSpec(("indexes",), frozenset({"dict"}), False, True),
                        ParameterSpec(
                            path=("indexes", ANY_STR), types=frozenset({"dict"}), is_required=False, _is_nullable=False
                        ),
                        ParameterSpec(("indexes", ANY_STR, "properties"), frozenset({"list"}), True, False),
                        ParameterSpec(("indexes", ANY_STR, "properties", ANY_INT), frozenset({"str"}), False, False),
                        ParameterSpec(("indexes", ANY_STR, "cursorable"), frozenset({"bool"}), False, False),
                    }
                ),
            ),
        ],
    )
    def test_read_as_expected(self, cls_: type, expected_parameters: ParameterSpecSet) -> None:
        actual_parameters = read_parameter_from_init_type_hints(cls_)

        assert sorted(actual_parameters) == sorted(expected_parameters)

    @pytest.mark.parametrize("loader_cls", RESOURCE_CRUD_LIST)
    def test_compatible_with_loaders(self, loader_cls: type[ResourceCRUD]) -> None:
        if loader_cls is LocationFilterCRUD:
            # TODO: https://cognitedata.atlassian.net/browse/CDF-22363
            pytest.skip(f"Skipping {loader_cls} because get_write_cls_parameter_spec fails for some reason")

        parameter_set = read_parameter_from_init_type_hints(loader_cls.resource_write_cls)

        assert isinstance(parameter_set, ParameterSpecSet)
        assert len(parameter_set) > 0


class TestReadParameterFromDict:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            (
                {"space": "space", "description": "description", "name": "name"},
                ParameterSet[ParameterValue](
                    {
                        ParameterValue(("space",), "str", "space"),
                        ParameterValue(("description",), "str", "description"),
                        ParameterValue(("name",), "str", "name"),
                    }
                ),
            ),
            (
                {
                    "externalId": "Asset",
                    "name": "Asset",
                    "space": "sp_asset_space",
                    "usedFor": "node",
                    "properties": {
                        "metadata": {
                            "type": {
                                "list": False,
                                "type": "json",
                            },
                            "nullable": False,
                            "autoIncrement": False,
                            "name": "name",
                            "defaultValue": "default_value",
                            "description": "description",
                        }
                    },
                },
                ParameterSet[ParameterValue](
                    {
                        ParameterValue(("externalId",), "str", "Asset"),
                        ParameterValue(("name",), "str", "Asset"),
                        ParameterValue(("space",), "str", "sp_asset_space"),
                        ParameterValue(("usedFor",), "str", "node"),
                        ParameterValue(("properties",), "dict", None),
                        ParameterValue(
                            (
                                "properties",
                                "metadata",
                            ),
                            "dict",
                            None,
                        ),
                        ParameterValue(("properties", "metadata", "type"), "dict", None),
                        ParameterValue(("properties", "metadata", "type", "list"), "bool", False),
                        ParameterValue(("properties", "metadata", "type", "type"), "str", "json"),
                        ParameterValue(("properties", "metadata", "nullable"), "bool", False),
                        ParameterValue(("properties", "metadata", "autoIncrement"), "bool", False),
                        ParameterValue(("properties", "metadata", "name"), "str", "name"),
                        ParameterValue(("properties", "metadata", "defaultValue"), "str", "default_value"),
                        ParameterValue(("properties", "metadata", "description"), "str", "description"),
                    }
                ),
            ),
            ({}, ParameterSet[ParameterValue]({})),
        ],
    )
    def test_read_expected(self, raw: dict, expected: ParameterSet[ParameterValue]):
        actual = read_parameters_from_dict(raw)

        assert sorted(actual) == sorted(expected)
