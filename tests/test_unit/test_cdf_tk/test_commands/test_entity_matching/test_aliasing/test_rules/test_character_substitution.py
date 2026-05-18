import re

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.character_substitution import (
    CharacterSubstitutionContext,
    CharacterSubstitutionContextBuilder,
    CharacterSubstitutionRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import MacroCallSignature


def normalize_expression(expression: str) -> str:
    return re.sub(r"v_[0-9a-f]{32}", "v_*", expression)


class TestCharacterSubstitutionContext:
    def test_when_valid_replacements_then_creation_succeeds(self) -> None:
        replacements = {"a": "b", "c": "d"}
        context = CharacterSubstitutionContext(replacements)
        assert context.replacements == replacements

    def test_when_single_replacement_then_creation_succeeds(self) -> None:
        replacements = {"a": "b"}
        context = CharacterSubstitutionContext(replacements)
        assert context.replacements == replacements

    def test_when_empty_replacements_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="replacements dictionary cannot be empty"):
            CharacterSubstitutionContext({})

    def test_when_empty_from_char_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="from_char cannot be empty"):
            CharacterSubstitutionContext({"": "b"})


class TestCharacterSubstitutionContextBuilder:
    def test_when_building_with_single_replacement_then_context_created(self) -> None:
        builder = CharacterSubstitutionContextBuilder()
        context = builder.add_replacement("a", "b").build()
        assert context.replacements == {"a": "b"}

    def test_when_building_with_multiple_replacements_then_all_added(self) -> None:
        builder = CharacterSubstitutionContextBuilder()
        context = builder.add_replacement("a", "b").add_replacement("c", "d").add_replacement("e", "f").build()
        assert context.replacements == {"a": "b", "c": "d", "e": "f"}

    def test_when_overriding_replacement_then_latest_value_used(self) -> None:
        builder = CharacterSubstitutionContextBuilder()
        context = builder.add_replacement("a", "b").add_replacement("a", "c").build()
        assert context.replacements == {"a": "c"}

    def test_when_building_without_replacements_then_raises_value_error(self) -> None:
        builder = CharacterSubstitutionContextBuilder()
        with pytest.raises(ValueError, match="replacements dictionary cannot be empty"):
            builder.build()

    def test_when_builder_returns_self_then_fluent_chaining_works(self) -> None:
        builder = CharacterSubstitutionContextBuilder()
        result = builder.add_replacement("a", "b")
        assert isinstance(result, CharacterSubstitutionContextBuilder)
        assert result is builder


class TestCharacterSubstitutionRuleDefinition:
    def test_when_calling_type_then_returns_correct_rule_type(self) -> None:
        rule_def = CharacterSubstitutionRuleDefinition()
        result = rule_def.type()
        assert result == RuleType.CHARACTER_SUBSTITUTION

    def test_when_single_replacement_then_macro_has_correct_definition(self) -> None:
        rule_def = CharacterSubstitutionRuleDefinition()
        context = CharacterSubstitutionContext({"a": "b"})

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = "(v_*) => v_*.map(char => char.replace('a', 'b'))"
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert "#test_macro := " in result
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"

    def test_when_multiple_replacements_then_macro_chains_all_replacements(self) -> None:
        rule_def = CharacterSubstitutionRuleDefinition()
        context = CharacterSubstitutionContext({"a": "b", "c": "d"})

        macro = rule_def.create_kuiper_macro(context)

        expected_lambda = "(v_*) => v_*.map(char => char.replace('a', 'b').replace('c', 'd'))"
        assert normalize_expression(macro.definition) == expected_lambda

        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)
        assert "#test_macro := " in result
        assert normalize_expression(result) == "#test_macro := " + expected_lambda + ";"
