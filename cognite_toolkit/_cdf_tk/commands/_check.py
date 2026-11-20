from collections.abc import Sequence
from datetime import timedelta
from enum import StrEnum
from pathlib import Path

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._validation import ValidationCommand
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import ValidationResultList
from cognite_toolkit._cdf_tk.data_classes import CommandDisplay
from cognite_toolkit._cdf_tk.data_classes._insights import InsightList
from cognite_toolkit._cdf_tk.rules import FailingJobRuleSet, ToolkitRuleSet


class CheckType(StrEnum):
    failing = "failing"
    all = "all"


class CheckCommand(ValidationCommand):
    def check(
        self,
        checks: Sequence[CheckType],
        client: ToolkitClient,
        lookback_period: int = 3,
    ) -> ValidationResultList:
        """Checks the resources in the CDF project.

        Args:
            checks: The checks to run.
            client: The ToolkitClient for the CDF project to check.
            lookback_period: The number of days back in time to look for failing jobs.
        """
        plan = self._create_check_plan(set(checks), client, lookback_period)
        validation_results = self.validate(client=client, display=CommandDisplay(results=False), plan=plan)
        BuildV2Command.display_insights(
            insights=InsightList([insight for result in validation_results for insight in result.insights]),
            insight_path=Path("dummy"),
            console=client.console,
            verbose=True,
            title=f"Check Results for CDF Project '{client.config.project}'",
        )
        return validation_results

    @classmethod
    def _create_check_plan(
        cls, checks: set[CheckType], client: ToolkitClient, lookback_period: int
    ) -> list[ToolkitRuleSet]:
        plan: list[ToolkitRuleSet] = []
        if checks & {CheckType.failing, CheckType.all}:
            plan.append(FailingJobRuleSet(client=client, lookback_period=timedelta(days=lookback_period)))
        return plan
