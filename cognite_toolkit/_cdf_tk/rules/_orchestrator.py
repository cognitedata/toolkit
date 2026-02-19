from collections.abc import Callable

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import Module
from cognite_toolkit._cdf_tk.rules._base import get_rules_registry


class RulesOrchestrator:
    def __init__(
        self,
        can_run_validator: Callable[[str, type], bool] | None = None,
        enable_alpha_validators: bool = False,
    ) -> None:
        self._can_run_validator = can_run_validator or (lambda code, issue_type: True)  # type: ignore
        self._enable_alpha_validators = enable_alpha_validators

    def run(self, module: Module) -> None:
        """Run all applicable rules on the provided modules and return a list of insights.


        Args:
            module: The module to run the rules on.

        Returns:
            A list of insights generated from the rules.
        """

        rules_registry = get_rules_registry()

        for resource_type, resources in module.resources_by_type.items():
            for rule in rules_registry.get(resource_type.kind, []):
                if rule.alpha and not self._enable_alpha_validators:
                    continue

                if self._can_run_validator(rule.code, rule.insight_type):
                    module.insights.extend(rule(resources).validate())
