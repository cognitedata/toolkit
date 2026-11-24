from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.commands import CheckCommand, CheckType
from cognite_toolkit._cdf_tk.rules import (
    FailingJobRuleSet,
    ToolkitRuleSet,
    TransformationRuleSet,
    UnusedRuleSet,
    get_global_rules_registry,
)

# TransformationRuleSet is scheduled for deletion and is not part of the check command.
EXCLUDED_CDF_RULE_SETS: set[type[ToolkitRuleSet]] = {TransformationRuleSet}


class TestCheckCommand:
    @pytest.mark.parametrize(
        "checks, expected",
        [
            pytest.param({CheckType.failing}, {FailingJobRuleSet}, id="failing"),
            pytest.param({CheckType.unused}, {UnusedRuleSet}, id="unused"),
            pytest.param(
                {CheckType.failing, CheckType.unused}, {FailingJobRuleSet, UnusedRuleSet}, id="failing and unused"
            ),
        ],
    )
    def test_create_check_plan(self, checks: set[CheckType], expected: set[type[ToolkitRuleSet]]) -> None:
        plan = CheckCommand._create_check_plan(checks, MagicMock(), lookback_period=5)

        assert {type(rule) for rule in plan} == expected

    @pytest.mark.parametrize(
        "checks",
        [
            pytest.param({CheckType.all}, id="all"),
            pytest.param({CheckType.failing, CheckType.all}, id="failing and all"),
        ],
    )
    def test_create_check_plan_all_includes_all_cdf_rule_sets(self, checks: set[CheckType]) -> None:
        expected = {
            rule_set
            for rule_set in get_global_rules_registry()
            if "cdf" in rule_set.CONTEXT and rule_set not in EXCLUDED_CDF_RULE_SETS
        }

        plan = CheckCommand._create_check_plan(checks, MagicMock(), lookback_period=5)

        plan_types = [type(rule) for rule in plan]
        assert len(plan_types) == len(set(plan_types)), "Each rule set should only be included once"
        assert set(plan_types) == expected

    def test_create_check_plan_passes_time_periods(self) -> None:
        plan = CheckCommand._create_check_plan({CheckType.all}, MagicMock(), lookback_period=5, min_age=10)

        periods = {
            type(rule): rule.lookback_period if isinstance(rule, FailingJobRuleSet) else rule.created_before
            for rule in plan
            if isinstance(rule, FailingJobRuleSet | UnusedRuleSet)
        }
        assert periods == {FailingJobRuleSet: timedelta(days=5), UnusedRuleSet: timedelta(days=10)}
