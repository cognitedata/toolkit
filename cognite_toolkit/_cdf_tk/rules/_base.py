from abc import ABC, abstractmethod
from typing import ClassVar

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, Recommendation
from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses


class ToolkitRule(ABC):
    """Rule for toolkit resource validation principles."""

    code: ClassVar[str]
    resource_type: ClassVar[str]
    insight_type: ClassVar[type[ConsistencyError] | type[Recommendation]]
    alpha: ClassVar[bool] = False
    fixable: ClassVar[bool] = False

    def __init__(
        self,
        resources: list[ToolkitResource],
    ) -> None:
        self.resources = resources

    @abstractmethod
    def validate(self) -> list[ConsistencyError] | list[Recommendation] | list[ConsistencyError | Recommendation]:
        """Execute rule validation."""
        ...


_registry: dict[str, list[type[ToolkitRule]]] | None = None


def get_rules_registry(force_reload: bool = False) -> dict[str, list[type[ToolkitRule]]]:
    global _registry
    if _registry is None or force_reload:
        registry: dict[str, list[type[ToolkitRule]]] = {}
        rules = get_concrete_subclasses(ToolkitRule)

        for rule_cls in rules:
            registry.setdefault(rule_cls.resource_type, []).append(rule_cls)
        _registry = registry
    return _registry
