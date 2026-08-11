from typing import Any
from unittest.mock import Mock

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import (
    Rule,
    RuleDefinition,
    RuleDescription,
    RuleName,
    RuleType,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro


class TestRuleType:
    def test_enum_value_is_correct(self) -> None:
        assert RuleType.CHARACTER_SUBSTITUTION == "character_substitution"


class TestRuleName:
    def test_when_valid_name_then_creation_succeeds(self) -> None:
        rule_name = RuleName("my_rule")
        assert rule_name.name == "my_rule"

    def test_when_empty_name_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Rule name cannot be empty"):
            RuleName("")

    def test_when_none_name_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Rule name cannot be empty"):
            RuleName(None)  # type: ignore[arg-type]


class TestRuleDescription:
    def test_when_valid_description_then_creation_succeeds(self) -> None:
        desc = RuleDescription("This is a test rule")
        assert desc.description == "This is a test rule"

    def test_when_empty_description_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Rule description cannot be empty"):
            RuleDescription("")

    def test_when_none_description_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="Rule description cannot be empty"):
            RuleDescription(None)  # type: ignore[arg-type]


class MockRuleDefinition(RuleDefinition):
    def __init__(self, rule_type: RuleType, macro: Macro):
        self._rule_type = rule_type
        self._macro = macro

    def type(self) -> RuleType:
        return self._rule_type

    def deserialize_context(self, payload: dict[str, Any]) -> Any:
        return {}

    def create_kuiper_macro(self, context: Any) -> Macro:
        return self._macro


class TestRule:
    def test_when_valid_components_then_creation_succeeds(self) -> None:
        name = RuleName("test_rule")
        description = RuleDescription("A test rule")
        macro = Macro(definition="test_def")
        rule_def = MockRuleDefinition(RuleType.CHARACTER_SUBSTITUTION, macro)

        rule = Rule(name, description, rule_def)
        assert rule.name == name
        assert rule.description == description
        assert rule.rule_definition == rule_def

    def test_when_using_factory_method_then_rule_created(self) -> None:
        name = RuleName("test_rule")
        description = RuleDescription("A test rule")
        macro = Macro(definition="test_def")
        rule_def = MockRuleDefinition(RuleType.CHARACTER_SUBSTITUTION, macro)

        rule = Rule.from_rule_definition(name, description, rule_def)
        assert rule.name == name
        assert rule.description == description
        assert rule.rule_definition == rule_def

    def test_when_calling_create_kuiper_macro_then_returns_macro(self) -> None:
        name = RuleName("test_rule")
        description = RuleDescription("A test rule")
        expected_macro = Macro(definition="(s) => s.replace('a', 'b')")
        rule_def = MockRuleDefinition(RuleType.CHARACTER_SUBSTITUTION, expected_macro)
        rule = Rule(name, description, rule_def)

        context = Mock()
        result = rule.create_kuiper_macro(context)

        assert result == expected_macro
