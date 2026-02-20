from collections.abc import Callable

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import Module
from cognite_toolkit._cdf_tk.rules._base import ToolkitRule
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

_rules_registry: dict[str, list[type[ToolkitRule]]] | None = None


def get_rules_registry(force_reload: bool = False) -> dict[str, list[type[ToolkitRule]]]:
    """Get the registry of rules, optionally forcing a reload.

    Args:
        force_reload: If True, forces a reload of the rules registry.

    Returns:
        A dictionary mapping resource types to lists of ToolkitRule classes.
    """
    global _rules_registry
    if _rules_registry is None or force_reload:
        registry: dict[str, list[type[ToolkitRule]]] = {}
        rules = get_concrete_subclasses(ToolkitRule)  # type: ignore

        for rule_cls in rules:
            registry.setdefault(rule_cls.resource_type, []).append(rule_cls)
        _rules_registry = registry
    return _rules_registry


class RulesOrchestrator:
    def __init__(
        self,
        can_run_validator: Callable[[str, type], bool] | None = None,
        enable_alpha_validators: bool = False,
    ) -> None:
        self._can_run_validator = can_run_validator or (lambda code, issue_type: True)
        self._enable_alpha_validators = enable_alpha_validators

    def run(self, module: Module) -> None:
        """Run all applicable rules on the provided modules while updating modules insights.

        Args:
            module: The module to run the rules on.
        """

        rules_registry = get_rules_registry()

        for resource_type, resources in module.resources_by_type.items():
            for rule in rules_registry.get(resource_type.kind, []):
                if rule.alpha and not self._enable_alpha_validators:
                    continue

                if self._can_run_validator(rule.code, rule.insight_type):
                    if insights := rule(resources).validate():
                        module.insights.extend(insights)
