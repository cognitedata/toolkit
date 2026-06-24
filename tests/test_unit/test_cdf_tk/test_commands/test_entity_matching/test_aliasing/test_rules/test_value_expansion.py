import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.value_expansion import (
    ValueExpansionContext,
    ValueExpansionContextBuilder,
    ValueExpansionRuleDefinition,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro, MacroCallSignature


class TestValueExpansionContext:
    def test_when_valid_expansions_then_creation_succeeds(self) -> None:
        expansions = {"P": ["PUMP", "PMP"], "M": ["MOTOR", "MOT"]}
        context = ValueExpansionContext(expansions)
        assert context.expansions == expansions

    def test_when_single_expansion_then_creation_succeeds(self) -> None:
        expansions = {"P": ["PUMP", "PMP"]}
        context = ValueExpansionContext(expansions)
        assert context.expansions == expansions

    def test_when_single_expansion_value_then_creation_succeeds(self) -> None:
        expansions = {"P": ["PUMP"]}
        context = ValueExpansionContext(expansions)
        assert context.expansions == expansions

    def test_when_empty_expansions_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="expansions dictionary cannot be empty"):
            ValueExpansionContext({})

    def test_when_empty_abbreviation_key_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="abbreviation key cannot be empty"):
            ValueExpansionContext({"": ["PUMP"]})

    def test_when_empty_expansion_list_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="expansion list cannot be empty"):
            ValueExpansionContext({"P": []})

    def test_when_empty_expansion_value_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="expansion value cannot be empty"):
            ValueExpansionContext({"P": ["PUMP", ""]})

    def test_when_expansion_not_list_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="expansion values must be lists"):
            ValueExpansionContext({"P": "PUMP"})  # type: ignore[dict-item]

    def test_when_expansion_is_dict_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="expansion values must be lists"):
            ValueExpansionContext({"P": {"pump": 1}})  # type: ignore[dict-item]


class TestValueExpansionContextBuilder:
    def test_when_building_with_single_expansion_then_context_created(self) -> None:
        builder = ValueExpansionContextBuilder()
        context = builder.add_expansion("P", ["PUMP", "PMP"]).build()
        assert context.expansions == {"P": ["PUMP", "PMP"]}

    def test_when_building_with_multiple_expansions_then_all_added(self) -> None:
        builder = ValueExpansionContextBuilder()
        context = (
            builder.add_expansion("P", ["PUMP", "PMP"])
            .add_expansion("M", ["MOTOR", "MOT"])
            .add_expansion("V", ["VALVE"])
            .build()
        )
        assert context.expansions == {
            "P": ["PUMP", "PMP"],
            "M": ["MOTOR", "MOT"],
            "V": ["VALVE"],
        }

    def test_when_overriding_expansion_then_latest_value_used(self) -> None:
        builder = ValueExpansionContextBuilder()
        context = builder.add_expansion("P", ["PUMP"]).add_expansion("P", ["PUMP", "PMP"]).build()
        assert context.expansions == {"P": ["PUMP", "PMP"]}

    def test_when_building_without_expansions_then_raises_value_error(self) -> None:
        builder = ValueExpansionContextBuilder()
        with pytest.raises(ValueError, match="expansions dictionary cannot be empty"):
            builder.build()

    def test_when_builder_returns_self_then_fluent_chaining_works(self) -> None:
        builder = ValueExpansionContextBuilder()
        result = builder.add_expansion("P", ["PUMP"])
        assert isinstance(result, ValueExpansionContextBuilder)
        assert result is builder


class TestValueExpansionRuleDefinition:
    def test_when_calling_type_then_returns_correct_rule_type(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        result = rule_def.type()
        assert result == RuleType.VALUE_EXPANSION

    def test_when_deserialize_with_valid_single_expansion_then_context_created(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        payload = {"expansions": {"P": ["PUMP", "PMP"]}}

        context = rule_def.deserialize_context(payload)

        assert context.expansions == {"P": ["PUMP", "PMP"]}

    def test_when_deserialize_with_multiple_expansions_then_all_deserialized(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        payload = {
            "expansions": {
                "P": ["PUMP", "PMP"],
                "M": ["MOTOR", "MOT"],
            }
        }

        context = rule_def.deserialize_context(payload)

        assert context.expansions == {
            "P": ["PUMP", "PMP"],
            "M": ["MOTOR", "MOT"],
        }

    def test_when_deserialize_missing_expansions_key_then_raises_value_error(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        payload = {"other_key": {"P": ["PUMP"]}}

        with pytest.raises(ValueError, match="ValueExpansion rule payload must contain 'expansions' key"):
            rule_def.deserialize_context(payload)

    def test_when_deserialize_expansions_not_dict_then_raises_value_error(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        payload = {"expansions": [["P", ["PUMP"]]]}

        with pytest.raises(ValueError, match="'expansions' must be a dictionary"):
            rule_def.deserialize_context(payload)

    def test_when_deserialize_expansion_value_not_list_then_raises_value_error(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        payload = {"expansions": {"P": "PUMP"}}

        with pytest.raises(ValueError, match="expansion for 'P' must be a list"):
            rule_def.deserialize_context(payload)

    def test_when_create_macro_with_single_expansion_then_generates_valid_macro(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        context = ValueExpansionContext({"P": ["PUMP", "PMP"]})

        macro = rule_def.create_kuiper_macro(context)

        assert isinstance(macro, Macro)
        assert isinstance(macro.definition, str)
        assert len(macro.definition) > 0
        assert "flatmap" in macro.definition
        assert "replace" in macro.definition

    def test_when_create_macro_with_multiple_expansions_then_all_included(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        context = ValueExpansionContext({"P": ["PUMP", "PMP"], "M": ["MOTOR"]})

        macro = rule_def.create_kuiper_macro(context)

        definition = macro.definition
        assert "PUMP" in definition
        assert "PMP" in definition
        assert "MOTOR" in definition
        assert "replace" in definition
        assert definition.count("replace") >= 2

    def test_when_macro_definition_not_empty(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        context = ValueExpansionContext({"P": ["PUMP"]})

        macro = rule_def.create_kuiper_macro(context)

        assert macro.definition != ""
        assert len(macro.definition) > 0

    def test_when_as_string_with_call_signature_then_formats_correctly(self) -> None:
        rule_def = ValueExpansionRuleDefinition()
        context = ValueExpansionContext({"P": ["PUMP"]})

        macro = rule_def.create_kuiper_macro(context)
        call_sig = MacroCallSignature("test_macro")
        result = macro.as_string(call_sig)

        assert "#test_macro :=" in result
        assert "flatmap" in result
        assert result.endswith(";")
