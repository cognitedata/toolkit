from __future__ import annotations

import pytest

from cognite_toolkit._cdf_tk._parameters import (
    ANY_STR,
    ANYTHING,
    ParameterSet,
    ParameterSpec,
    ParameterSpecSet,
    ParameterValue,
)

SPACE_SPEC = ParameterSpecSet(
    {
        ParameterSpec(("space",), frozenset({"str"}), True, False),
        ParameterSpec(("description",), frozenset({"str"}), False, True),
        ParameterSpec(("name",), frozenset({"str"}), False, True),
    }
)

DATASET_SPEC = ParameterSpecSet(
    {
        ParameterSpec(path=("description",), types=frozenset({"str"}), is_required=False, _is_nullable=True),
        ParameterSpec(path=("name",), types=frozenset({"str"}), is_required=False, _is_nullable=True),
        ParameterSpec(path=("externalId",), types=frozenset({"str"}), is_required=False, _is_nullable=True),
        ParameterSpec(path=("metadata", ANY_STR), types=frozenset({"str"}), is_required=False, _is_nullable=False),
        ParameterSpec(path=("metadata",), types=frozenset({"dict"}), is_required=False, _is_nullable=True),
        ParameterSpec(path=("writeProtected",), types=frozenset({"bool"}), is_required=False, _is_nullable=True),
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
                SPACE_SPEC.required(),
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
            pytest.param(
                ParameterSet[ParameterValue](
                    {
                        ParameterValue(path=("name",), type="str", value="B_5rVE|'2@vQnR!)%s|Cnoz^(.yY%`[3HfoXFO6&\\"),
                        ParameterValue(path=("writeProtected",), type="bool", value=True),
                        ParameterValue(path=("description",), type="str", value="e(Ox6G"),
                        ParameterValue(
                            path=(
                                "metadata",
                                '"u&SjvpP%<aS*wdOi$4Cuy2_p,?V2f9xN:*W/I$6<ZntaN=l9yKWE-To^sJ=`ltyykBN|]zv^(B-P)y%.h.")',
                            ),
                            type="str",
                            value="#7x,",
                        ),
                        ParameterValue(path=("metadata",), type="dict", value=None),
                        ParameterValue(
                            path=("externalId",), type="str", value="WTNVF3N27L9HX6D3OCZMO22X3VQ82LY6QJHCSWUBOXWYWHKQ62"
                        ),
                    }
                ),
                DATASET_SPEC,
                ParameterSet[ParameterValue](),
                id="No difference DataSet, with AnyStr",
            ),
            pytest.param(
                ParameterSet[ParameterValue](
                    {
                        ParameterValue(path=("name",), type="str", value="name"),
                        ParameterValue(
                            path=(
                                "metadata",
                                "some key",
                            ),
                            type="str",
                            value="#7x,",
                        ),
                        ParameterValue(
                            path=("metadata", "some key", "type"),
                            type="str",
                            value="json",
                        ),
                        ParameterValue(path=("metadata",), type="dict", value=None),
                    }
                ),
                ParameterSpecSet(
                    {
                        ParameterSpec(
                            path=("metadata", ANYTHING),
                            types=frozenset({"dict"}),
                            is_required=False,
                            _is_nullable=False,
                        ),
                    }
                ),
                ParameterSet[ParameterValue](
                    {
                        ParameterValue(path=("name",), type="str", value="name"),
                        ParameterValue(path=("metadata",), type="dict", value=None),
                    }
                ),
                id="Diff with Anything in spec",
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
