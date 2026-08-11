import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.rules_discovery import LocalRulesDiscovery
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.character_substitution import (
    CharacterSubstitutionRuleDefinition,
)


@pytest.fixture
def discovery() -> LocalRulesDiscovery:
    return LocalRulesDiscovery()


@pytest.fixture
def discovered_rules(discovery: LocalRulesDiscovery) -> dict[RuleType, RuleDefinition]:
    return discovery.discover_rules()


@pytest.fixture
def expected_rule_definitions() -> dict[RuleType, type[CharacterSubstitutionRuleDefinition]]:
    return {
        RuleType.CHARACTER_SUBSTITUTION: CharacterSubstitutionRuleDefinition,
    }


class TestLocalRulesDiscoveryBasics:
    def test_when_all_expected_rules_present_then_discovered(
        self,
        discovered_rules: dict[RuleType, RuleDefinition],
        expected_rule_definitions: dict[RuleType, type[CharacterSubstitutionRuleDefinition]],
    ) -> None:
        for rule_type in expected_rule_definitions.keys():
            assert rule_type in discovered_rules

    def test_when_discover_rules_then_all_have_valid_rule_types(
        self, discovered_rules: dict[RuleType, RuleDefinition]
    ) -> None:
        assert len(discovered_rules) >= 1
        assert all(isinstance(rule_type, RuleType) for rule_type in discovered_rules.keys())
        assert all(isinstance(rule_def, RuleDefinition) for rule_def in discovered_rules.values())


class TestLocalRulesDiscoveryParametrized:
    @pytest.mark.parametrize(
        "rule_type,rule_def_class",
        [
            (RuleType.CHARACTER_SUBSTITUTION, CharacterSubstitutionRuleDefinition),
        ],
        ids=lambda x: str(x).split(".")[-1] if hasattr(x, "__name__") else x.value if hasattr(x, "value") else str(x),
    )
    def test_when_rule_type_discovered_then_is_correct_class(
        self,
        discovered_rules: dict[RuleType, RuleDefinition],
        rule_type: RuleType,
        rule_def_class: type[CharacterSubstitutionRuleDefinition],
    ) -> None:
        assert rule_type in discovered_rules
        assert isinstance(discovered_rules[rule_type], rule_def_class)

    @pytest.mark.parametrize(
        "rule_type,rule_def_class",
        [
            (RuleType.CHARACTER_SUBSTITUTION, CharacterSubstitutionRuleDefinition),
        ],
        ids=lambda x: str(x).split(".")[-1] if hasattr(x, "__name__") else x.value if hasattr(x, "value") else str(x),
    )
    def test_when_rule_discovered_then_type_method_returns_correct_value(
        self,
        discovered_rules: dict[RuleType, RuleDefinition],
        rule_type: RuleType,
        rule_def_class: type[CharacterSubstitutionRuleDefinition],
    ) -> None:
        rule_def = discovered_rules[rule_type]
        assert rule_def.type() == rule_type
