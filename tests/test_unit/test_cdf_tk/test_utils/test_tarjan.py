from collections.abc import Iterable

import pytest

from cognite_toolkit._cdf_tk.utils.tarjan import tarjan


def tarjan_test_cases() -> Iterable:
    yield pytest.param(
        {},
        [],
        id="Empty graph",
    )
    yield pytest.param(
        {"A": set()},
        [{"A"}],
        id="Single node",
    )
    yield pytest.param(
        {"A": {"B"}, "B": set()},
        [{"B"}, {"A"}],
        id="Two nodes",
    )
    yield pytest.param(
        {"A": {"B"}, "B": {"A"}},
        [{"A", "B"}],
        id="Circular dependency",
    )
    yield pytest.param(
        {"A": {"B"}, "B": {"C"}, "C": {"A"}},
        [{"C", "B", "A"}],
        id="Circular dependency with three nodes",
    )


class TestTarjan:
    @pytest.mark.parametrize("graph, expected", tarjan_test_cases())
    def test_tarjan(self, graph: dict[str, set[str]], expected: list[set[str]]):
        result = tarjan(graph)
        assert result == expected
