from __future__ import annotations

from typing import Any

import pytest

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ANY_STR


class TestAnyStrInt:
    @pytest.mark.parametrize("other", [1, "a", None])
    def test_any_str_equals(self, other: Any) -> None:
        is_string = isinstance(other, str)
        is_equal = ANY_STR == other

        assert is_string == is_equal

    @pytest.mark.parametrize("other", [1, "a", None])
    def test_any_int_equals(self, other: Any) -> None:
        is_int = isinstance(other, int)
        is_equal = ANY_INT == other

        assert is_int == is_equal

    @pytest.mark.parametrize(
        "dump, spec, expected",
        [
            (("metadata",), ("metadata", ANY_STR), False),
            (("metadata",), ("metadata", ANY_INT), False),
            (("metadata", 0), ("metadata", ANY_INT), True),
            (("metadata",), ("metadata",), True),
            (("metadata",), ("metadata", "a"), False),
            (("metadata", "a"), ("metadata", ANY_STR), True),
        ],
    )
    def test_any_str_tuple_equals(self, dump: tuple, spec: tuple, expected: bool) -> None:
        actual = dump == spec

        assert actual == expected

    def test_less_than(self) -> None:
        is_less_than = ANY_INT < "str"
        assert is_less_than is True

    def test_less_than_reverse(self) -> None:
        is_less_than = "str" < ANY_INT
        assert is_less_than is False
