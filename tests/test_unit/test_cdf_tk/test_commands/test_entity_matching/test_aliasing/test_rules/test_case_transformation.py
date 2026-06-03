import re

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.case_transformation import (
    CaseStrategy,
    CaseTransformationContext,
    CaseTransformationRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import MacroCallSignature


def normalize_expression(expression: str) -> str:
    return re.sub(r"v_[0-9a-f]{32}", "v_*", expression)


class TestCaseStrategy:
    def test_when_uppercase_strategy_then_value_is_correct(self) -> None:
        assert CaseStrategy.UPPERCASE.value == "UPPERCASE"

    def test_when_lowercase_strategy_then_value_is_correct(self) -> None:
        assert CaseStrategy.LOWERCASE.value == "LOWERCASE"

    def test_when_all_strategies_then_count_is_two(self) -> None:
        strategies = list(CaseStrategy)
        assert len(strategies) == 2


class TestCaseTransformationContext:
    def test_when_uppercase_strategy_then_creation_succeeds(self) -> None:
        context = CaseTransformationContext(strategy=CaseStrategy.UPPERCASE)
        assert context.strategy == CaseStrategy.UPPERCASE

    def test_when_lowercase_strategy_then_creation_succeeds(self) -> None:
        context = CaseTransformationContext(strategy=CaseStrategy.LOWERCASE)
        assert context.strategy == CaseStrategy.LOWERCASE


class TestCaseTransformationRuleDefinition:
    def test_when_calling_type_then_returns_correct_rule_type(self) -> None:
        rule_def = CaseTransformationRuleDefinition()
        result = rule_def.type()
        assert result == RuleType.CASE_TRANSFORMATION

    def test_when_deserializing_with_uppercase_then_context_created(self) -> None:
        rule_def = CaseTransformationRuleDefinition()
        payload = {"strategy": "UPPERCASE"}
        context = rule_def.deserialize_context(payload)
        assert context.strategy == CaseStrategy.UPPERCASE

    def test_when_deserializing_with_lowercase_then_context_created(self) -> None:
        rule_def = CaseTransformationRuleDefinition()
        payload = {"strategy": "LOWERCASE"}
        context = rule_def.deserialize_context(payload)
        assert context.strategy == CaseStrategy.LOWERCASE

    def test_when_deserializing_without_strategy_then_raises_value_error(self) -> None:
        rule_def = CaseTransformationRuleDefinition()
        payload: dict[str, str] = {}
        with pytest.raises(ValueError, match="CaseTransformation rule payload must contain 'strategy' key"):
            rule_def.deserialize_context(payload)

    def test_when_deserializing_with_non_string_strategy_then_raises_value_error(self) -> None:
        rule_def = CaseTransformationRuleDefinition()
        payload = {"strategy": 123}
        with pytest.raises(ValueError, match="'strategy' must be a string"):
            rule_def.deserialize_context(payload)

    def test_when_deserializing_with_invalid_strategy_then_raises_value_error(self) -> None:
        rule_def = CaseTransformationRuleDefinition()
        payload = {"strategy": "INVALID_STRATEGY"}
        with pytest.raises(ValueError, match="'strategy' must be one of: UPPERCASE, LOWERCASE"):
            rule_def.deserialize_context(payload)

    def test_when_uppercase_strategy_then_macro_has_correct_definition(self) -> None:
        rule_def = CaseTransformationRuleDefinition()
        context = CaseTransformationContext(strategy=CaseStrategy.UPPERCASE)

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = "(v_*) => v_*.map(value => upper(value))"
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"

    def test_when_lowercase_strategy_then_macro_has_correct_definition(self) -> None:
        rule_def = CaseTransformationRuleDefinition()
        context = CaseTransformationContext(strategy=CaseStrategy.LOWERCASE)

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = "(v_*) => v_*.map(value => lower(value))"
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"
