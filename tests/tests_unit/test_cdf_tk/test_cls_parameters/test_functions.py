import pytest
from cognite.client.data_classes.data_modeling import ContainerApply, SpaceApply

from cognite_toolkit._cdf_tk.load import RESOURCE_LOADER_LIST, ResourceLoader
from cognite_toolkit._cdf_tk.load._cls_parameters import (
    ParameterSpec,
    ParameterSpecSet,
    read_parameter_from_init_type_hints,
)


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
                        ParameterSpec(("properties", "type"), frozenset({"dict"}), True, False),
                        ParameterSpec(("properties", "nullable"), frozenset({"bool"}), False, False),
                        ParameterSpec(("properties", "auto_increment"), frozenset({"bool"}), False, False),
                        ParameterSpec(("properties", "name"), frozenset({"str"}), False, True),
                        ParameterSpec(("properties", "default_value"), frozenset({"str"}), False, True),
                        ParameterSpec(("properties", "description"), frozenset({"str"}), False, True),
                        ParameterSpec(("properties", "type", "collation"), frozenset({"str"}), False, False),
                        ParameterSpec(("properties", "type", "container"), frozenset({"dict"}), False, True),
                        ParameterSpec(("properties", "type", "container", "space"), frozenset({"str"}), True, False),
                        ParameterSpec(
                            ("properties", "type", "container", "external_id"), frozenset({"str"}), True, False
                        ),
                        ParameterSpec(("properties", "type", "is_list"), frozenset({"bool"}), False, False),
                        ParameterSpec(("properties", "type", "unit"), frozenset({"dict"}), False, True),
                        ParameterSpec(("properties", "type", "unit", "external_id"), frozenset({"str"}), True, False),
                        ParameterSpec(("properties", "type", "unit", "source_unit"), frozenset({"str"}), False, True),
                        ParameterSpec(("description",), frozenset({"str"}), False, True),
                        ParameterSpec(("name",), frozenset({"str"}), False, True),
                        ParameterSpec(("used_for",), frozenset({"str"}), False, True),
                        ParameterSpec(("constraints",), frozenset({"dict"}), False, True),
                        ParameterSpec(
                            (
                                "constraints",
                                "require",
                            ),
                            frozenset({"dict"}),
                            True,
                            False,
                        ),
                        ParameterSpec(("constraints", "require", "space"), frozenset({"str"}), True, False),
                        ParameterSpec(("constraints", "require", "external_id"), frozenset({"str"}), True, False),
                        ParameterSpec(("constraints", "properties"), frozenset({"list"}), True, False),
                        ParameterSpec(("constraints", "properties", 0), frozenset({"str"}), True, False),
                        ParameterSpec(("indexes",), frozenset({"dict"}), False, True),
                        ParameterSpec(("indexes", "properties"), frozenset({"list"}), True, False),
                        ParameterSpec(("indexes", "properties", 0), frozenset({"str"}), True, False),
                        ParameterSpec(("indexes", "cursorable"), frozenset({"bool"}), False, False),
                    }
                ),
            ),
        ],
    )
    def test_read_as_expected(self, cls_: type, expected_parameters: ParameterSpecSet) -> None:
        actual_parameters = read_parameter_from_init_type_hints(cls_)

        assert sorted(actual_parameters) == sorted(expected_parameters)

    @pytest.mark.parametrize("loader_cls", RESOURCE_LOADER_LIST)
    def test_compatible_with_loaders(self, loader_cls: type[ResourceLoader]) -> None:
        parameter_set = read_parameter_from_init_type_hints(loader_cls.resource_write_cls)

        assert isinstance(parameter_set, ParameterSpecSet)
        assert len(parameter_set) > 0
