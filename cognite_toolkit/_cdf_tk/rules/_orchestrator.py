from collections.abc import Set

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import Module
from cognite_toolkit._cdf_tk.rules._base import ToolkitGlobalRule, ToolkitLocalRule, ToolkitRule
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

_LOCAL_RULES_REGISTRY: list[type[ToolkitLocalRule]] | None = None
_GLOBAL_RULES_REGISTRY: list[type[ToolkitGlobalRule]] | None = None


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


def get_global_rules_registry(force_reload: bool = False) -> list[type[ToolkitGlobalRule]]:
    """Get the registry of rules, optionally forcing a reload."""
    global _GLOBAL_RULES_REGISTRY
    if _GLOBAL_RULES_REGISTRY is None or force_reload:
        _GLOBAL_RULES_REGISTRY = list(get_concrete_subclasses(ToolkitGlobalRule))  # type: ignore[type-abstract]
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


class GlobalRulesOrchestrator(RuleOrchestrator):
    # Todo: invent a better data structure, to include which rules are skipped, and which
    def run(self, modules: list[BuiltModule], client: ToolkitRule) -> dict[str, list[Insight]]:
        rues_registry = get_global_rules_registry()
        all_insights: dict[str, list[Insight]] = {}

        for rule_cls in rues_registry:
            if not self.can_run_rule(rule_cls):
                continue
            if rule_cls.REQUIRES_CLIENT and not rule_cls.REQUIRES_CLIENT:
                continue
            rule = rule_cls(modules)
            try:
                all_insights[rule_cls.__name__] = list(rule.validate())
            except Exception as e:
                # This runts Neat plugin, so need to not crash even if neat does.
                raise e
        return all_insights
