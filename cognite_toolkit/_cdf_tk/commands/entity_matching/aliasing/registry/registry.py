from abc import ABC, abstractmethod
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.rules_discovery import (
    LocalRulesDiscovery,
    RulesDiscovery,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


class RuleDefinitionRegistry(ABC):
    @abstractmethod
    def get_definition_or_throw(self, rule_type: RuleType) -> RuleDefinition[Any]:
        pass


class RuleDefinitionNotFoundError(ToolkitValueError):
    def __init__(self, rule_type: RuleType) -> None:
        self.rule_type = rule_type
        super().__init__(f"Rule type {rule_type.value} not found in registry")


class LocalRuleDefinitionRegistry(RuleDefinitionRegistry):
    def __init__(self, definitions: dict[RuleType, RuleDefinition[Any]]) -> None:
        self._definitions = definitions

    @staticmethod
    def bootstrap(rules_discovery: RulesDiscovery | None = None) -> "LocalRuleDefinitionRegistry":
        if rules_discovery is None:
            rules_discovery = LocalRulesDiscovery.create()
        definitions = rules_discovery.discover_rules()
        return LocalRuleDefinitionRegistry(definitions)

    def get_definition_or_throw(self, rule_type: RuleType) -> RuleDefinition[Any]:
        if rule_type not in self._definitions:
            raise RuleDefinitionNotFoundError(rule_type)
        return self._definitions[rule_type]
