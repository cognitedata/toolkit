from collections.abc import Iterable

import pytest
from _pytest.mark import ParameterSet
from cognite.client.data_classes.capabilities import Capability

from cognite_toolkit._cdf_tk.client.data_classes.capabilities import scope_intersection, scope_union


def scope_logic_test_cases() -> Iterable[ParameterSet]:
    yield pytest.param(None, None, None, None, id="Two None scopes")


class TestScopeLogic:
    @pytest.mark.parametrize("scope1, scope2, intersection, union", list(scope_logic_test_cases()))
    def test_scope_intersection(
        self,
        scope1: Capability.Scope | None,
        scope2: Capability | None,
        intersection: Capability.Scope | None,
        union: Capability.Scope | None,
    ) -> None:
        result = scope_intersection(scope1, scope2)
        assert result == intersection

    @pytest.mark.parametrize("scope1, scope2, intersection, union", list(scope_logic_test_cases()))
    def test_scope_union(
        self,
        scope1: Capability.Scope | None,
        scope2: Capability | None,
        intersection: Capability.Scope | None,
        union: Capability.Scope | None,
    ) -> None:
        result = scope_union(scope1, scope2)
        assert result == union
