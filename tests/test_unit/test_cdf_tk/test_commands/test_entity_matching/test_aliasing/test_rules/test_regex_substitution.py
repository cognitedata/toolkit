import re

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.regex_substitution import (
    RegExpSubstitutionContext,
    RegExpSubstitutionContextBuilder,
    RegExpSubstitutionRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import MacroCallSignature


def normalize_expression(expression: str) -> str:
    return re.sub(r"v_[0-9a-f]{32}", "v_*", expression)


class TestRegExpSubstitutionContext:
    def test_when_valid_pattern_and_replacement_then_creation_succeeds(self) -> None:
        pattern = "^ASSET_"
        replacement = "ALT_ASSET_"
        context = RegExpSubstitutionContext(pattern=pattern, replacement=replacement)
        assert context.pattern == pattern
        assert context.replacement == replacement

    def test_when_simple_pattern_then_creation_succeeds(self) -> None:
        context = RegExpSubstitutionContext(pattern="test", replacement="result")
        assert context.pattern == "test"
        assert context.replacement == "result"

    def test_when_empty_pattern_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="pattern cannot be empty"):
            RegExpSubstitutionContext(pattern="", replacement="result")

    def test_when_empty_replacement_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="replacement cannot be empty"):
            RegExpSubstitutionContext(pattern="test", replacement="")

    def test_when_special_regex_characters_in_pattern_then_creation_succeeds(self) -> None:
        pattern = r"^\w+\.\w+$"
        replacement = "MATCH"
        context = RegExpSubstitutionContext(pattern=pattern, replacement=replacement)
        assert context.pattern == pattern
        assert context.replacement == replacement


class TestRegExpSubstitutionContextBuilder:
    def test_when_building_with_pattern_and_replacement_then_context_created(self) -> None:
        builder = RegExpSubstitutionContextBuilder()
        context = builder.with_pattern("test").with_replacement("result").build()
        assert context.pattern == "test"
        assert context.replacement == "result"

    def test_when_builder_returns_self_then_fluent_chaining_works(self) -> None:
        builder = RegExpSubstitutionContextBuilder()
        result = builder.with_pattern("test")
        assert isinstance(result, RegExpSubstitutionContextBuilder)
        assert result is builder

    def test_when_building_without_pattern_then_raises_value_error(self) -> None:
        builder = RegExpSubstitutionContextBuilder()
        with pytest.raises(ValueError, match="pattern must be set before building"):
            builder.with_replacement("result").build()

    def test_when_building_without_replacement_then_raises_value_error(self) -> None:
        builder = RegExpSubstitutionContextBuilder()
        with pytest.raises(ValueError, match="replacement must be set before building"):
            builder.with_pattern("test").build()

    def test_when_overriding_pattern_then_latest_value_used(self) -> None:
        builder = RegExpSubstitutionContextBuilder()
        context = builder.with_pattern("first").with_pattern("second").with_replacement("result").build()
        assert context.pattern == "second"

    def test_when_overriding_replacement_then_latest_value_used(self) -> None:
        builder = RegExpSubstitutionContextBuilder()
        context = builder.with_pattern("test").with_replacement("first").with_replacement("second").build()
        assert context.replacement == "second"


class TestRegExpSubstitutionRuleDefinition:
    def test_when_calling_type_then_returns_correct_rule_type(self) -> None:
        rule_def = RegExpSubstitutionRuleDefinition()
        result = rule_def.type()
        assert result == RuleType.REGEX_SUBSTITUTION

    def test_when_simple_pattern_and_replacement_then_macro_has_correct_definition(self) -> None:
        rule_def = RegExpSubstitutionRuleDefinition()
        context = RegExpSubstitutionContext(pattern="test", replacement="result")

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = "(v_*) => v_*.map(value => regex_replace(value, 'test', 'result'))"
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"

    def test_when_regex_pattern_with_special_chars_then_macro_preserves_pattern(self) -> None:
        rule_def = RegExpSubstitutionRuleDefinition()
        pattern = r"^\w+_"
        replacement = "PREFIX_"
        context = RegExpSubstitutionContext(pattern=pattern, replacement=replacement)

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = f"(v_*) => v_*.map(value => regex_replace(value, '{pattern}', '{replacement}'))"
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"

    def test_when_deserializing_valid_payload_then_context_created(self) -> None:
        rule_def = RegExpSubstitutionRuleDefinition()
        payload = {"pattern": "^ASSET_", "replacement": "ALT_ASSET_"}

        context = rule_def.deserialize_context(payload)

        assert context.pattern == "^ASSET_"
        assert context.replacement == "ALT_ASSET_"

    def test_when_deserializing_payload_without_pattern_then_raises_value_error(self) -> None:
        rule_def = RegExpSubstitutionRuleDefinition()
        payload = {"replacement": "result"}

        with pytest.raises(ValueError, match="'pattern' key"):
            rule_def.deserialize_context(payload)

    def test_when_deserializing_payload_without_replacement_then_raises_value_error(self) -> None:
        rule_def = RegExpSubstitutionRuleDefinition()
        payload = {"pattern": "test"}

        with pytest.raises(ValueError, match="'replacement' key"):
            rule_def.deserialize_context(payload)

    def test_when_deserializing_non_string_pattern_then_raises_value_error(self) -> None:
        rule_def = RegExpSubstitutionRuleDefinition()
        payload = {"pattern": 123, "replacement": "result"}

        with pytest.raises(ValueError, match="'pattern' must be a string"):
            rule_def.deserialize_context(payload)

    def test_when_deserializing_non_string_replacement_then_raises_value_error(self) -> None:
        rule_def = RegExpSubstitutionRuleDefinition()
        payload = {"pattern": "test", "replacement": 456}

        with pytest.raises(ValueError, match="'replacement' must be a string"):
            rule_def.deserialize_context(payload)
