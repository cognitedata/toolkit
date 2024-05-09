from __future__ import annotations

from typing import Any, Literal, Union

import pytest

from cognite_toolkit._cdf_tk.load._cls_parameters.type_hint import TypeHint


class TestTypeHint:
    @pytest.mark.parametrize(
        "raw, types, is_base_type, is_nullable, is_class, is_dict_type, is_list_type",
        [
            (str, ["str"], True, False, True, False, False),
            (Literal["a", "b"], ["str"], True, False, False, False, False),
            (str | None, ["str"], True, True, False, False, False),
            (Union[str, int], ["str", "int"], True, False, False, False, False),
            (str | int | bool, ["str", "int", "bool"], True, False, False, False, False),
            (dict[str, int], ["dict"], False, False, False, True, False),
            (list[str] | None, ["list"], False, True, False, False, True),
            (list[str] | dict[str, int] | None, ["list", "dict"], False, True, False, True, True),
        ],
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
        assert hint.types == types
        assert hint.is_base_type == is_base_type
        assert hint.is_nullable == is_nullable
        assert hint.is_class == is_class
        assert hint.is_dict_type == is_dict_type
        assert hint.is_list_type == is_list_type
