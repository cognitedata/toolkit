from datetime import timedelta
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.commands import CheckCommand, CheckType
from cognite_toolkit._cdf_tk.rules import FailingJobRuleSet


class TestCheckCommand:
    @pytest.mark.parametrize(
        "checks",
        [
            pytest.param({CheckType.failing}, id="failing"),
            pytest.param({CheckType.all}, id="all"),
            pytest.param({CheckType.failing, CheckType.all}, id="failing and all"),
        ],
    )
    def test_create_check_plan_includes_failing_jobs(self, checks: set[CheckType]) -> None:
        plan = CheckCommand._create_check_plan(checks, MagicMock(), lookback_period=5)

        assert [
            (type(rule), rule.lookback_period if isinstance(rule, FailingJobRuleSet) else None) for rule in plan
        ] == [(FailingJobRuleSet, timedelta(days=5))]
