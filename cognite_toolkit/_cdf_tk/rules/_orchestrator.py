from collections.abc import Set

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import Module
from cognite_toolkit._cdf_tk.rules._base import ToolkitGlobalRulSet, ToolkitLocalRule, ToolkitRule
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

_LOCAL_RULES_REGISTRY: list[type[ToolkitLocalRule]] | None = None
_GLOBAL_RULES_REGISTRY: list[type[ToolkitGlobalRulSet]] | None = None


def get_local_rules_registry(force_reload: bool = False) -> list[type[ToolkitLocalRule]]:
    """Get the registry of rules, optionally forcing a reload.

    Args:
        force_reload: If True, forces a reload of the rules registry.

    Returns:
        A dictionary mapping resource types to lists of ToolkitRule classes.
    """
    global _LOCAL_RULES_REGISTRY
    if _LOCAL_RULES_REGISTRY is None or force_reload:
        _LOCAL_RULES_REGISTRY = list(get_concrete_subclasses(ToolkitLocalRule))  # type: ignore[type-abstract]
    return _LOCAL_RULES_REGISTRY


def get_global_rules_registry(force_reload: bool = False) -> list[type[ToolkitGlobalRulSet]]:
    """Get the registry of rules, optionally forcing a reload."""
    global _GLOBAL_RULES_REGISTRY
    if _GLOBAL_RULES_REGISTRY is None or force_reload:
        _GLOBAL_RULES_REGISTRY = list(get_concrete_subclasses(ToolkitGlobalRulSet))  # type: ignore[type-abstract]
    return _GLOBAL_RULES_REGISTRY


class RuleOrchestrator:
    def __init__(self, exclude_rule_codes: Set[str] | None = None, enable_alpha_validators: bool = False) -> None:
        self.exclude_rule_codes = exclude_rule_codes or set()
        self._enable_alpha_validators = enable_alpha_validators

    def can_run_rule(self, rule_cls: type[ToolkitRule]) -> bool:
        if rule_cls.code in self.exclude_rule_codes:
            return False
        if not rule_cls.alpha:
            return True
        # Alpha Rule
        return self._enable_alpha_validators


class LocalRulesOrchestrator(RuleOrchestrator):
    def run(self, module: Module) -> list[Insight]:
        """Run all applicable rules on the provided modules while updating modules insights.

        Args:
            module: The module to run the rules on.
        """

        rules_registry = get_local_rules_registry()
        all_insights: list[Insight] = []
        for rule_cls in rules_registry:
            if not self.can_run_rule(rule_cls):
                continue
            rule = rule_cls(module)
            all_insights.extend(rule.validate())
        return all_insights
