from typing import Any

import pytest

from cognite_toolkit._cdf_tk.load._cls_parameters.type_hint import TypeHint


class TestTypeHint:
    @pytest.mark.parametrize(
        "raw, as_str, is_base_type, is_nullable, is_class, is_dict_type, is_list_type",
        [
            (str, "str", True, False, True, False, False),
        ],
    )
    def test_type_hint(
        self,
        raw: Any,
        as_str: str,
        is_base_type: bool,
        is_nullable: bool,
        is_class: bool,
        is_dict_type: bool,
        is_list_type: bool,
    ):
        hint = TypeHint(raw)
        assert str(hint) == as_str
        assert hint.is_base_type == is_base_type
        assert hint.is_nullable == is_nullable
        assert hint.is_class == is_class
        assert hint.is_dict_type == is_dict_type
        assert hint.is_list_type == is_list_type
