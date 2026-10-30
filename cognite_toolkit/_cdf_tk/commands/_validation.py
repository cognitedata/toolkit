from collections import Counter
from dataclasses import dataclass

from rich.console import Console
from rich.progress import Progress

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuiltModule,
    InsightList,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import (
    ValidationResult,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    Insight,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitValidationError,
)
from cognite_toolkit._cdf_tk.rules import ToolkitGlobalRuleSet, get_global_rules_registry
from cognite_toolkit._cdf_tk.rules._base import EXECUTE_RULE_STATUS, RuleSetStatus
from cognite_toolkit._cdf_tk.ui import AuraColor, ToolkitPanel, ToolkitTable


@dataclass
class ValidationStep:
    status: RuleSetStatus
    rule: ToolkitGlobalRuleSet


class ValidationCommand(ToolkitCommand):
    def validate(
        self, modules: list[BuiltModule], client: ToolkitClient | None = None, display: bool = True
    ) -> list[ValidationResult]:
        """Validates the built modules in the build folder."""
        console = client.console if client else Console(markup=True)
        plan = self._create_validation_plan(modules, client)
        if display:
            self._display_validation_plan(plan, console)
        return self._run_validation(plan, console)

    @classmethod
    def _create_validation_plan(
        cls, built_modules: list[BuiltModule], client: ToolkitClient | None
    ) -> list[ValidationStep]:
        all_rules = get_global_rules_registry()
        plan: list[ValidationStep] = []
        for rule_cls in all_rules:
            rule = rule_cls(built_modules, client=client)

            status = rule.get_status()
            plan.append(ValidationStep(status=status, rule=rule))
        return plan

    @classmethod
    def _display_validation_plan(cls, plan: list[ValidationStep], console: Console) -> None:
        skip_count = sum(1 for step in plan if step.status.code == "skip")
        unavailable_count = sum(1 for step in plan if step.status.code == "unavailable")

        border_color = 0
        if skip_count:
            border_color = max(border_color, 1)
        if unavailable_count:
            border_color = max(border_color, 1)

        table = ToolkitTable(*["Validating", "Status", "Message"])
        for step in plan:
            status_style = {"ready": "green", "reduced": "yellow", "skip": "yellow", "unavailable": "red"}[
                step.status.code
            ]
            status_display = f"[{status_style}]{step.status.code.capitalize()}[/]"
            message = step.status.message or "-"
            table.add_row(step.rule.DISPLAY_NAME, status_display, message)

        border_style = {0: AuraColor.GREEN.rich, 1: AuraColor.AMBER.rich, 2: AuraColor.RED.rich}[border_color]

        console.print(
            ToolkitPanel(
                table,
                title="Planning validation",
                border_style=border_style,
            )
        )

    @classmethod
    def _run_validation(cls, plan: list[ValidationStep], console: Console) -> list[ValidationResult]:
        with Progress(console=console) as progress:
            ready_step_count = sum(1 for step in plan if step.status.code in EXECUTE_RULE_STATUS)
            validating_task = progress.add_task("Checking modules", total=ready_step_count)
            validation_results: list[ValidationResult] = []
            for step in plan:
                if step.status.code not in EXECUTE_RULE_STATUS:
                    continue
                display_name = step.rule.DISPLAY_NAME
                progress.update(validating_task, description=f"Running '{display_name}'...")

                insights: list[Insight] = []
                errors: list[InternalValidatorException] = []
                for result in step.rule.validate():
                    if isinstance(result, InternalValidatorException):
                        errors.append(result)
                    else:
                        insights.append(result)

                validation_results.append(ValidationResult(name=display_name, insights=insights, errors=errors))
                progress.update(
                    validating_task,
                    advance=1,
                    description=f"Finished validating {display_name}. Found {len(insights)} insights.",
                )
            progress.update(validating_task, description=f"Finished validating. Ran {ready_step_count} validations.")
        return validation_results

    @classmethod
    def _enforce_rules(cls, violations: InsightList) -> None:
        """Fails the build when blocking rule violations are found.

        Args:
            violations: The insights reported to the user (already filtered for ignored rules).

        Raises:
            ToolkitValidationError: If any blocking rule violations are found, with a summary of the violations.
        """
        if not violations:
            return
        counts_by_code = Counter(insight.code or "UNDEFINED" for insight in violations)
        raise ToolkitValidationError(
            f"Build failed due to rule enforcement. Found {len(violations)} blocking rule violation(s) "
            f"across {len(counts_by_code)} rule code(s). See the insights above for details."
        )
