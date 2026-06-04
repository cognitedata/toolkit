import re

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.prefix_suffix import (
    PrefixSuffixContext,
    PrefixSuffixRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import MacroCallSignature


def normalize_expression(expression: str) -> str:
    return re.sub(r"v_[0-9a-f]{32}", "v_*", expression)


class TestPrefixSuffixContext:
    def test_when_both_prefix_and_suffix_provided_then_creation_succeeds(self) -> None:
        context = PrefixSuffixContext(prefix="PRE_", suffix="_SUF")
        assert context.prefix == "PRE_"
        assert context.suffix == "_SUF"

    def test_when_only_prefix_provided_then_creation_succeeds(self) -> None:
        context = PrefixSuffixContext(prefix="PRE_", suffix=None)
        assert context.prefix == "PRE_"
        assert context.suffix is None

    def test_when_only_suffix_provided_then_creation_succeeds(self) -> None:
        context = PrefixSuffixContext(prefix=None, suffix="_SUF")
        assert context.prefix is None
        assert context.suffix == "_SUF"

    def test_when_both_prefix_and_suffix_are_none_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="At least one of prefix or suffix must be provided and non-empty"):
            PrefixSuffixContext(prefix=None, suffix=None)

    def test_when_both_prefix_and_suffix_are_empty_strings_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="At least one of prefix or suffix must be provided and non-empty"):
            PrefixSuffixContext(prefix="", suffix="")

    def test_when_prefix_empty_and_suffix_none_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="At least one of prefix or suffix must be provided and non-empty"):
            PrefixSuffixContext(prefix="", suffix=None)

    def test_when_prefix_none_and_suffix_empty_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="At least one of prefix or suffix must be provided and non-empty"):
            PrefixSuffixContext(prefix=None, suffix="")


class TestPrefixSuffixRuleDefinition:
    def test_when_calling_type_then_returns_correct_rule_type(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        result = rule_def.type()
        assert result == RuleType.PREFIX_SUFFIX

    def test_when_deserializing_with_prefix_only_then_context_created(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        payload = {"prefix": "PRE_"}
        context = rule_def.deserialize_context(payload)
        assert context.prefix == "PRE_"
        assert context.suffix is None

    def test_when_deserializing_with_suffix_only_then_context_created(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        payload = {"suffix": "_SUF"}
        context = rule_def.deserialize_context(payload)
        assert context.prefix is None
        assert context.suffix == "_SUF"

    def test_when_deserializing_with_both_prefix_and_suffix_then_context_created(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        payload = {"prefix": "PRE_", "suffix": "_SUF"}
        context = rule_def.deserialize_context(payload)
        assert context.prefix == "PRE_"
        assert context.suffix == "_SUF"

    def test_when_deserializing_with_empty_payload_then_raises_value_error(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        payload: dict[str, str] = {}
        with pytest.raises(ValueError, match="At least one of prefix or suffix must be provided and non-empty"):
            rule_def.deserialize_context(payload)

    def test_when_deserializing_with_non_string_prefix_then_raises_value_error(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        payload = {"prefix": 123, "suffix": "_SUF"}
        with pytest.raises(ValueError, match="'prefix' must be a string"):
            rule_def.deserialize_context(payload)

    def test_when_deserializing_with_non_string_suffix_then_raises_value_error(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        payload = {"prefix": "PRE_", "suffix": 456}
        with pytest.raises(ValueError, match="'suffix' must be a string"):
            rule_def.deserialize_context(payload)

    def test_when_prefix_only_then_macro_has_correct_definition(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        context = PrefixSuffixContext(prefix="PRE_", suffix=None)

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = '(v_*) => v_*.map(value => concat("PRE_", value))'
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"

    def test_when_suffix_only_then_macro_has_correct_definition(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        context = PrefixSuffixContext(prefix=None, suffix="_SUF")

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = '(v_*) => v_*.map(value => concat(value, "_SUF"))'
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"

    def test_when_both_prefix_and_suffix_then_macro_has_correct_definition(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        context = PrefixSuffixContext(prefix="PRE_", suffix="_SUF")

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = '(v_*) => v_*.map(value => concat("PRE_", value, "_SUF"))'
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"

    def test_when_prefix_with_special_characters_then_macro_generated_correctly(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        context = PrefixSuffixContext(prefix="[PREFIX]", suffix=None)

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = '(v_*) => v_*.map(value => concat("[PREFIX]", value))'
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"

    def test_when_suffix_with_special_characters_then_macro_generated_correctly(self) -> None:
        rule_def = PrefixSuffixRuleDefinition()
        context = PrefixSuffixContext(prefix=None, suffix="[SUFFIX]")

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = '(v_*) => v_*.map(value => concat(value, "[SUFFIX]"))'
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"
