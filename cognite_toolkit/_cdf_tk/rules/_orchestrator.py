from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Insight
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import Module
from cognite_toolkit._cdf_tk.rules._base import ToolkitGlobalRuleSet, ToolkitLocalRule
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

_LOCAL_RULES_REGISTRY: list[type[ToolkitLocalRule]] | None = None
_GLOBAL_RULES_REGISTRY: list[type[ToolkitGlobalRuleSet]] | None = None


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


def get_global_rules_registry(force_reload: bool = False) -> list[type[ToolkitGlobalRuleSet]]:
    """Get the registry of rules, optionally forcing a reload."""
    global _GLOBAL_RULES_REGISTRY
    if _GLOBAL_RULES_REGISTRY is None or force_reload:
        _GLOBAL_RULES_REGISTRY = list(get_concrete_subclasses(ToolkitGlobalRuleSet))  # type: ignore[type-abstract]
    return _GLOBAL_RULES_REGISTRY


class LocalRulesOrchestrator:
    def run(self, module: Module) -> list[Insight]:
        """Run all applicable rules on the provided modules while updating modules insights.

        Args:
            module: The module to run the rules on.
        """

        rules_registry = get_local_rules_registry()
        all_insights: list[Insight] = []
        for rule_cls in rules_registry:
            rule = rule_cls(module)
            all_insights.extend(rule.validate())
        return all_insights
