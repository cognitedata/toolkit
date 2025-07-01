from __future__ import annotations

import collections.abc
import enum
import sys
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, List, Literal, Union  # noqa: UP035

import pytest
from _pytest.mark import ParameterSet
from cognite.client.data_classes.data_modeling import DirectRelationReference, NodeId
from cognite.client.data_classes.data_modeling.instances import PropertyValue

from cognite_toolkit._cdf_tk._parameters.type_hint import TypeHint


class Action(enum.Enum):
    Read = "READ"
    Execute = "EXECUTE"
    List = "LIST"


def type_hint_test_cases() -> Iterable[ParameterSet]:
    # "raw, types, is_base_type, is_nullable, is_class, is_dict_type, is_list_type",
    yield pytest.param(str, ["str"], True, False, False, False, False, id="str")
    yield pytest.param(Literal["a", "b"], ["str"], True, False, False, False, False, id="Literal")
    yield pytest.param(dict[str, int], ["dict"], False, False, False, True, False, id="dict")
    yield pytest.param(Union[str, int], ["str", "int"], True, False, False, False, False, id="Union")
    yield pytest.param(dict, ["dict"], False, False, False, True, False, id="dict without type hints")
    yield pytest.param(Any, ["unknown"], False, False, False, False, False, id="Any")
    yield pytest.param(Action, ["str"], True, False, True, False, False, id="Enum")
    yield pytest.param(
        Union[
            str,
            int,
            float,
            bool,
            dict,
            List[str],  # noqa: UP006
            List[int],  # noqa: UP006
            List[float],  # noqa: UP006
            List[bool],  # noqa: UP006
            List[dict],  # noqa: UP006
            NodeId,
            DirectRelationReference,
        ],
        ["str", "int", "float", "bool", "dict", "list"],
        True,
        False,
        True,
        True,
        True,
        id="Union with almost all possible types",
    )
    yield pytest.param(Sequence[int], ["list"], False, False, False, False, True, id="Sequence")
    yield pytest.param(Mapping[str, PropertyValue], ["dict"], False, False, False, True, False, id="Mapping")
    yield pytest.param(
        collections.abc.Mapping[str, PropertyValue], ["dict"], False, False, False, True, False, id="ABC Mapping"
    )
    yield pytest.param(str | None, ["str"], True, True, False, False, False, id="str | None")
    yield pytest.param(
        str | int | bool, ["str", "int", "bool"], True, False, False, False, False, id="str | int | bool"
    )
    yield pytest.param(list[str] | None, ["list"], False, True, False, False, True, id="list | None")
    yield pytest.param(
        list[str] | dict[str, int] | None, ["list", "dict"], False, True, False, True, True, id="list | dict | None"
    )


@pytest.mark.skipif(sys.version_info[:2] == (3, 10), reason="Fails for Python 3.10, tech debt to fix")
class TestTypeHint:
    @pytest.mark.parametrize(
        "raw, types, is_base_type, is_nullable, is_class, is_dict_type, is_list_type",
        list(type_hint_test_cases()),
    )
    def test_type_hint(
        self,
        raw: Any,
        types: list[str],
        is_base_type: bool,
        is_nullable: bool,
        is_class: bool,
        is_dict_type: bool,
        is_list_type: bool,
    ):
        hint = TypeHint(raw)
        assert set(hint.types) == set(types)
        assert hint.is_base_type == is_base_type
        assert hint.is_nullable == is_nullable
        assert hint.is_user_defined_class == is_class
        assert hint.is_dict_type == is_dict_type
        assert hint.is_list_type == is_list_type
