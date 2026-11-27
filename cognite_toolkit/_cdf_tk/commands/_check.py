from collections.abc import Sequence
from datetime import timedelta
from enum import StrEnum
from pathlib import Path

from rich.markup import escape

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._validation import ValidationCommand
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import ValidationResultList
from cognite_toolkit._cdf_tk.data_classes import CommandDisplay
from cognite_toolkit._cdf_tk.data_classes._insights import InsightList
from cognite_toolkit._cdf_tk.rules import FailingJobRuleSet, ToolkitRuleSet, UnusedRuleSet


class CheckType(StrEnum):
    failing = "failing"
    unused = "unused"
    all = "all"


class CheckCommand(ValidationCommand):
    def check(
        self,
        checks: Sequence[CheckType],
        client: ToolkitClient,
        lookback_period: int = 3,
        min_age: int = 14,
    ) -> ValidationResultList:
        """Checks the resources in the CDF project.

        Args:
            checks: The checks to run.
            client: The ToolkitClient for the CDF project to check.
            lookback_period: The number of days back in time to look for failing jobs.
            min_age: The minimum number of days since a resource was created for it to be reported as unused.
        """
        plan = self._create_check_plan(set(checks), client, lookback_period, min_age)
        validation_results = self.validate(client=client, display=CommandDisplay(results=False), plan=plan)
        validation_errors = [error for result in validation_results for error in result.errors]
        if validation_errors:
            client.console.print(f"[bold red]Error:[/] Found {len(validation_errors)} internal validator exception(s):")
            for result in validation_results:
                for error in result.errors:
                    client.console.print(f"  - {result.name}: [red]{escape(error.message)}[/]")
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
        cls, checks: set[CheckType], client: ToolkitClient, lookback_period: int, min_age: int = 14
    ) -> list[ToolkitRuleSet]:
        plan: list[ToolkitRuleSet] = []
        if checks & {CheckType.failing, CheckType.all}:
            plan.append(FailingJobRuleSet(client=client, lookback_period=timedelta(days=lookback_period)))
        if checks & {CheckType.all, CheckType.unused}:
            plan.append(UnusedRuleSet(client=client, created_before=timedelta(days=min_age)))
        return plan
