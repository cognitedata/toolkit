from __future__ import annotations

import pytest

from cognite_toolkit._cdf_tk._cls_parameters import ParameterSet, ParameterSpec, ParameterSpecSet, ParameterValue

SPACE_SPEC = ParameterSpecSet(
    {
        ParameterSpec(("space",), frozenset({"str"}), True, False),
        ParameterSpec(("description",), frozenset({"str"}), False, True),
        ParameterSpec(("name",), frozenset({"str"}), False, True),
    }
)


class TestSetOperations:
    @pytest.mark.parametrize(
        "first, second, expected",
        [
            pytest.param(
                ParameterSet[ParameterValue](
                    {
                        ParameterValue(("space",), "str", "space"),
                        ParameterValue(("description",), "str", "description"),
                        ParameterValue(("name",), "str", "name"),
                    }
                ),
                SPACE_SPEC,
                ParameterSet[ParameterValue]({}),
                id="Value - Spec no difference",
            ),
            pytest.param(
                SPACE_SPEC,
                ParameterSet[ParameterValue](
                    {
                        ParameterValue(("space",), "str", "space"),
                        ParameterValue(("description",), "str", "description"),
                        ParameterValue(("name",), "str", "name"),
                    }
                ),
                ParameterSpecSet({}),
                id="Spec - Value no difference",
            ),
            pytest.param(
                SPACE_SPEC.required,
                ParameterSet[ParameterValue](),
                ParameterSpecSet({ParameterSpec(("space",), frozenset({"str"}), True, False)}),
                id="Missing required",
            ),
            pytest.param(
                ParameterSet[ParameterValue](
                    {
                        ParameterValue(("space",), "str", "space"),
                        ParameterValue(("wrong_name",), "str", "description"),
                    }
                ),
                SPACE_SPEC,
                ParameterSet[ParameterValue]({ParameterValue(("wrong_name",), "str", "description")}),
                id="One unexpected value",
            ),
        ],
    )
    def test_difference(self, first: ParameterSet, second: ParameterSet, expected: ParameterSet) -> None:
        actual = first - second

        assert sorted(actual) == sorted(expected)

    @pytest.mark.parametrize(
        "my_set, path, expected",
        [
            pytest.param(
                SPACE_SPEC,
                ("space",),
                ParameterSpecSet({ParameterSpec(("space",), frozenset({"str"}), True, False)}),
                id="Subset tuple",
            ),
            pytest.param(
                ParameterSpecSet[ParameterValue](
                    {
                        ParameterValue(("space",), "str", "space"),
                        ParameterValue(("properties", "type"), "str", "description"),
                        ParameterValue(("name",), "str", "name"),
                    }
                ),
                1,
                ParameterSpecSet[ParameterValue](
                    {
                        ParameterValue(("space",), "str", "space"),
                        ParameterValue(("name",), "str", "name"),
                    }
                ),
                id="One unexpected value",
            ),
        ],
    )
    def test_subset(self, my_set: ParameterSet, path: int | tuple[str | int, ...], expected: ParameterSet) -> None:
        actual = my_set.subset(path)

        assert sorted(actual) == sorted(expected)
