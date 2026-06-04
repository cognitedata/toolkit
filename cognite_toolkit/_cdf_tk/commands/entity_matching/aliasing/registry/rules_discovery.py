from abc import ABC, abstractmethod
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.case_transformation import (
    CaseTransformationRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.character_substitution import (
    CharacterSubstitutionRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.composite import CompositeRuleDefinition
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.leading_zero_normalization import (
    LeadingZeroNormalizationRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.prefix_suffix import PrefixSuffixRuleDefinition
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.regex_substitution import (
    RegExpSubstitutionRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.value_expansion import ValueExpansionRuleDefinition


class RulesDiscovery(ABC):
    @abstractmethod
    def discover_rules(self) -> dict[RuleType, RuleDefinition[Any]]:
        pass


class LocalRulesDiscovery(RulesDiscovery):
    @staticmethod
    def create() -> "LocalRulesDiscovery":
        return LocalRulesDiscovery()

    def discover_rules(self) -> dict[RuleType, RuleDefinition[Any]]:
        definitions: list[RuleDefinition[Any]] = [
            CharacterSubstitutionRuleDefinition(),
            RegExpSubstitutionRuleDefinition(),
            PrefixSuffixRuleDefinition(),
            CaseTransformationRuleDefinition(),
            ValueExpansionRuleDefinition(),
            LeadingZeroNormalizationRuleDefinition(),
            CompositeRuleDefinition(),
        ]
        return {definition.type(): definition for definition in definitions}
