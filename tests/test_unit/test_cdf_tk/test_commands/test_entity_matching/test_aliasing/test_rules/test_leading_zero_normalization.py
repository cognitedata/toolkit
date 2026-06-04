import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.leading_zero_normalization import (
    LeadingZeroNormalizationContext,
    LeadingZeroNormalizationRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro, MacroCallSignature


class TestLeadingZeroNormalizationContext:
    def test_when_valid_target_length_zero_then_creation_succeeds(self) -> None:
        context = LeadingZeroNormalizationContext(target_length=0)
        assert context.target_length == 0

    def test_when_valid_target_length_positive_then_creation_succeeds(self) -> None:
        context = LeadingZeroNormalizationContext(target_length=5)
        assert context.target_length == 5

    def test_when_valid_target_length_large_then_creation_succeeds(self) -> None:
        context = LeadingZeroNormalizationContext(target_length=100)
        assert context.target_length == 100

    def test_when_negative_target_length_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="target_length cannot be negative"):
            LeadingZeroNormalizationContext(target_length=-1)

    def test_when_target_length_is_none_then_raises_type_error(self) -> None:
        with pytest.raises(TypeError):
            LeadingZeroNormalizationContext(target_length=None)  # type: ignore[arg-type]


class TestLeadingZeroNormalizationRuleDefinition:
    def test_when_calling_type_then_returns_correct_rule_type(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        result = rule_def.type()
        assert result == RuleType.LEADING_ZERO_NORMALIZATION

    def test_when_deserialize_with_valid_target_length_then_context_created(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        payload = {"target_length": 5}

        context = rule_def.deserialize_context(payload)

        assert context.target_length == 5

    def test_when_deserialize_with_zero_target_length_then_context_created(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        payload = {"target_length": 0}

        context = rule_def.deserialize_context(payload)

        assert context.target_length == 0

    def test_when_deserialize_missing_target_length_key_then_raises_value_error(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        payload = {"other_key": 5}

        with pytest.raises(ValueError, match="LeadingZeroNormalization rule payload must contain 'target_length' key"):
            rule_def.deserialize_context(payload)

    def test_when_deserialize_target_length_is_string_then_raises_value_error(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        payload = {"target_length": "5"}

        with pytest.raises(ValueError, match="'target_length' must be an integer"):
            rule_def.deserialize_context(payload)

    def test_when_deserialize_target_length_is_float_then_raises_value_error(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        payload = {"target_length": 5.5}

        with pytest.raises(ValueError, match="'target_length' must be an integer"):
            rule_def.deserialize_context(payload)

    def test_when_deserialize_negative_target_length_then_raises_value_error(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        payload = {"target_length": -1}

        with pytest.raises(ValueError, match="'target_length' cannot be negative"):
            rule_def.deserialize_context(payload)

    def test_when_create_macro_then_generates_valid_macro(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        context = LeadingZeroNormalizationContext(target_length=5)

        macro = rule_def.create_kuiper_macro(context)

        assert isinstance(macro, Macro)
        assert isinstance(macro.definition, str)
        assert len(macro.definition) > 0

    def test_when_create_macro_with_zero_target_length_then_expression_valid(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        context = LeadingZeroNormalizationContext(target_length=0)

        macro = rule_def.create_kuiper_macro(context)

        assert isinstance(macro.definition, str)
        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert "#test_macro :=" in result

    def test_when_create_macro_then_expression_contains_macro_definition(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        context = LeadingZeroNormalizationContext(target_length=3)

        macro = rule_def.create_kuiper_macro(context)

        expression = macro.definition
        assert "(v_" in expression
        assert ".map(value =>" in expression
        assert "substring" in expression

    def test_when_create_macro_with_large_target_length_then_expression_includes_padding(self) -> None:
        rule_def = LeadingZeroNormalizationRuleDefinition()
        context = LeadingZeroNormalizationContext(target_length=10)

        macro = rule_def.create_kuiper_macro(context)

        expression = macro.definition
        assert "0000000000" in expression
