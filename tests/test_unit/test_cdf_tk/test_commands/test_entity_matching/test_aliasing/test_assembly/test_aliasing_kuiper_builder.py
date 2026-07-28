from unittest.mock import Mock

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import (
    AliasingRule,
    DefaultAliasingKuiperBuilder,
    DuplicateRuleNameError,
    EmptyRulesError,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composer import ExpressionComposer
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.registry import (
    RuleDefinitionNotFoundError,
    RuleDefinitionRegistry,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro


class TestDefaultAliasingKuiperBuilder:
    def test_when_with_rule_called_then_returns_self_for_chaining(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule = AliasingRule("rule1", "character_substitution", "Test rule", {"replacements": {"a": "b"}})

        result = builder.with_rule(rule)

        assert result is builder

    def test_when_build_called_without_rules_then_raises_empty_rules_error(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)

        with pytest.raises(EmptyRulesError):
            builder.build()

    def test_when_build_called_with_duplicate_rule_names_then_raises_duplicate_error(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule1 = AliasingRule("rule1", "character_substitution", "Test 1", {"replacements": {"a": "b"}})
        rule2 = AliasingRule("rule1", "character_substitution", "Test 2", {"replacements": {"c": "d"}})

        builder.with_rule(rule1).with_rule(rule2)

        with pytest.raises(DuplicateRuleNameError, match="rule1"):
            builder.build()

    def test_when_build_called_with_multiple_duplicate_names_then_reports_all(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        builder.with_rule(AliasingRule("dup1", "character_substitution", "1", {"replacements": {"a": "b"}}))
        builder.with_rule(AliasingRule("dup1", "character_substitution", "2", {"replacements": {"c": "d"}}))
        builder.with_rule(AliasingRule("dup2", "character_substitution", "3", {"replacements": {"e": "f"}}))
        builder.with_rule(AliasingRule("dup2", "character_substitution", "4", {"replacements": {"g": "h"}}))

        with pytest.raises(DuplicateRuleNameError) as exc_info:
            builder.build()

        assert "dup1" in str(exc_info.value)
        assert "dup2" in str(exc_info.value)

    def test_when_build_called_with_single_valid_rule_then_returns_kuiper_with_expression(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_macro = Macro(definition="(s) => s.replace('a', 'b')")
        mock_definition.deserialize_context.return_value = Mock()
        mock_definition.create_kuiper_macro.return_value = mock_macro

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        expected_expression = "#rule1 := (s) => s.replace('a', 'b'); input.keys.map(...)"
        mock_composer.compose.return_value = expected_expression

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule = AliasingRule(
            name="test_rule",
            rule_type="character_substitution",
            description="Replace a with b",
            payload={"replacements": {"a": "b"}},
        )

        builder.with_rule(rule)
        result = builder.build()

        expected_result = AliasingKuiper(expression=expected_expression)
        assert result == expected_result

    def test_when_build_called_with_multiple_rules_then_returns_kuiper_with_composed_expression(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_macro1 = Macro(definition="(s) => s.replace('a', 'b')")
        mock_macro2 = Macro(definition="(s) => s.replace('c', 'd')")

        mock_definition.deserialize_context.return_value = Mock()
        mock_definition.create_kuiper_macro.side_effect = [mock_macro1, mock_macro2]

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        expected_expression = (
            "#rule1 := (s) => s.replace('a', 'b'); #rule2 := (s) => s.replace('c', 'd'); "
            "#composite_aliases := (candidates) => [rule1(candidates), rule2(candidates)]..."
        )
        mock_composer.compose.return_value = expected_expression

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Replace a with b",
            payload={"replacements": {"a": "b"}},
        )
        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Replace c with d",
            payload={"replacements": {"c": "d"}},
        )

        builder.with_rule(rule1).with_rule(rule2)
        result = builder.build()

        expected_result = AliasingKuiper(expression=expected_expression)
        assert result == expected_result

    def test_when_build_called_with_unregistered_rule_type_then_propagates_registry_error(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.side_effect = RuleDefinitionNotFoundError(RuleType.CHARACTER_SUBSTITUTION)
        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule = AliasingRule("rule1", "unknown_type", "Test", {"replacements": {}})

        builder.with_rule(rule)

        with pytest.raises(RuleDefinitionNotFoundError):
            builder.build()

    def test_when_build_called_with_invalid_payload_then_propagates_deserialization_error(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_definition.deserialize_context.side_effect = ValueError("replacements key is required")

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Test",
            payload={"invalid": "structure"},
        )

        builder.with_rule(rule)

        with pytest.raises(ValueError, match="replacements"):
            builder.build()

    def test_when_build_delegates_deserialization_to_rule_definition(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_context = Mock()
        mock_macro = Macro(definition="(s) => s")
        mock_definition.deserialize_context.return_value = mock_context
        mock_definition.create_kuiper_macro.return_value = mock_macro

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        expected_expression = "#test := (s) => s; input.keys.map(...)"
        mock_composer.compose.return_value = expected_expression

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule = AliasingRule("rule1", "custom_type", "Test", {"custom": "payload"})

        builder.with_rule(rule)
        result = builder.build()

        expected_result = AliasingKuiper(expression=expected_expression)
        assert result == expected_result

    def test_when_build_called_with_three_rules_then_composes_all_macros(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_macro1 = Macro(definition="(s) => s")
        mock_macro2 = Macro(definition="(s) => s")
        mock_macro3 = Macro(definition="(s) => s")

        mock_definition.create_kuiper_macro.side_effect = [mock_macro1, mock_macro2, mock_macro3]
        mock_definition.deserialize_context.return_value = Mock()

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        expected_expression = "composed_expression"
        mock_composer.compose.return_value = expected_expression

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule1 = AliasingRule("rule1", "type1", "desc1", {"key1": "val1"})
        rule2 = AliasingRule("rule2", "type2", "desc2", {"key2": "val2"})
        rule3 = AliasingRule("rule3", "type3", "desc3", {"key3": "val3"})

        builder.with_rule(rule1).with_rule(rule2).with_rule(rule3)
        result = builder.build()

        expected_result = AliasingKuiper(expression=expected_expression)
        assert result == expected_result

    def test_when_build_validates_all_rules_before_processing(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        builder.with_rule(AliasingRule("dup", "character_substitution", "1", {"replacements": {"a": "b"}}))
        builder.with_rule(AliasingRule("unique", "character_substitution", "2", {"replacements": {"c": "d"}}))
        builder.with_rule(AliasingRule("dup", "character_substitution", "3", {"replacements": {"e": "f"}}))

        with pytest.raises(DuplicateRuleNameError, match="dup"):
            builder.build()

    def test_when_rule_name_uniqueness_is_case_sensitive(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_macro1 = Macro(definition="(s) => s")
        mock_macro2 = Macro(definition="(s) => s")

        mock_definition.create_kuiper_macro.side_effect = [mock_macro1, mock_macro2]
        mock_definition.deserialize_context.return_value = Mock()

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        expected_expression = "expression_with_case_sensitive_rules"
        mock_composer.compose.return_value = expected_expression

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        rule1 = AliasingRule("Rule1", "character_substitution", "Test 1", {"replacements": {"a": "b"}})
        rule2 = AliasingRule("rule1", "character_substitution", "Test 2", {"replacements": {"c": "d"}})

        builder.with_rule(rule1).with_rule(rule2)
        result = builder.build()

        expected_result = AliasingKuiper(expression=expected_expression)
        assert result == expected_result

    def test_when_created_with_custom_registry_then_uses_provided_registry(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        assert builder._registry is mock_registry

    def test_when_created_with_custom_composer_then_uses_provided_composer(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)
        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        assert builder._composer is mock_composer


class TestCompositeRuleExpansion:
    def test_when_composite_rule_with_two_sub_rules_then_expands_and_processes_all(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_macro1 = Macro(definition="(s) => s.map(v => v + '_1')")
        mock_macro2 = Macro(definition="(s) => s.map(v => v + '_2')")

        mock_definition.deserialize_context.return_value = Mock()
        mock_definition.create_kuiper_macro.side_effect = [mock_macro1, mock_macro2]

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        expected_expression = "composed_expression"
        mock_composer.compose.return_value = expected_expression

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        composite_rule = AliasingRule(
            name="composite_rule",
            rule_type="composite",
            description="Test composite rule",
            payload={
                "rules": [
                    {"rule_type": "prefix_suffix", "payload": {"prefix": "P"}},
                    {"rule_type": "case_transformation", "payload": {"case": "upper"}},
                ]
            },
        )

        builder.with_rule(composite_rule)
        result = builder.build()

        assert mock_registry.get_definition_or_throw.call_count >= 2
        assert mock_composer.compose.call_count == 1
        compose_args = mock_composer.compose.call_args
        assert len(compose_args[0][0]) == 2

        expected_result = AliasingKuiper(expression=expected_expression)
        assert result == expected_result

    def test_when_composite_rule_with_three_sub_rules_then_all_expanded(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_macros = [
            Macro(definition="(s) => s.map(v => v + '_1')"),
            Macro(definition="(s) => s.map(v => v + '_2')"),
            Macro(definition="(s) => s.map(v => v + '_3')"),
        ]

        mock_definition.deserialize_context.return_value = Mock()
        mock_definition.create_kuiper_macro.side_effect = mock_macros

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        expected_expression = "composed_expression"
        mock_composer.compose.return_value = expected_expression

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        composite_rule = AliasingRule(
            name="composite_rule",
            rule_type="composite",
            description="Test composite rule with 3 sub-rules",
            payload={
                "rules": [
                    {"rule_type": "rule1", "payload": {"key": "val1"}},
                    {"rule_type": "rule2", "payload": {"key": "val2"}},
                    {"rule_type": "rule3", "payload": {"key": "val3"}},
                ]
            },
        )

        builder.with_rule(composite_rule).build()

        # Verify that all 3 sub-rules were processed
        assert mock_registry.get_definition_or_throw.call_count >= 3
        compose_args = mock_composer.compose.call_args
        assert len(compose_args[0][0]) == 3

    def test_when_composite_rule_mixed_with_regular_rules_then_expands_composite(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        mock_macros = [
            Macro(definition="(s) => s.map(v => v + '_regular1')"),
            Macro(definition="(s) => s.map(v => v + '_sub1')"),
            Macro(definition="(s) => s.map(v => v + '_sub2')"),
            Macro(definition="(s) => s.map(v => v + '_regular2')"),
        ]

        mock_definition.deserialize_context.return_value = Mock()
        mock_definition.create_kuiper_macro.side_effect = mock_macros

        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_registry.get_definition_or_throw.return_value = mock_definition

        mock_composer = Mock(spec=ExpressionComposer)
        expected_expression = "composed_expression"
        mock_composer.compose.return_value = expected_expression

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)

        regular_rule1 = AliasingRule("rule1", "prefix_suffix", "First", {"prefix": "A"})
        composite_rule = AliasingRule(
            name="composite",
            rule_type="composite",
            description="Test composite",
            payload={
                "rules": [
                    {"rule_type": "rule_a", "payload": {"key": "a"}},
                    {"rule_type": "rule_b", "payload": {"key": "b"}},
                ]
            },
        )
        regular_rule2 = AliasingRule("rule2", "case_transformation", "Second", {"case": "upper"})

        builder.with_rule(regular_rule1).with_rule(composite_rule).with_rule(regular_rule2).build()

        assert mock_registry.get_definition_or_throw.call_count >= 4
        compose_args = mock_composer.compose.call_args
        assert len(compose_args[0][0]) == 4

    def test_when_composite_rule_with_missing_rules_key_then_raises_value_error(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        composite_rule = AliasingRule(
            name="bad_composite",
            rule_type="composite",
            description="Malformed composite",
            payload={"wrong_key": []},  # Missing 'rules' key
        )

        builder.with_rule(composite_rule)

        with pytest.raises(ValueError, match="missing 'rules' key"):
            builder.build()

    def test_when_composite_rule_with_invalid_sub_rule_spec_then_raises_value_error(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)
        composite_rule = AliasingRule(
            name="bad_composite",
            rule_type="composite",
            description="Malformed composite",
            payload={
                "rules": [
                    {"rule_type": "rule1"},  # Missing 'payload' key
                ]
            },
        )

        builder.with_rule(composite_rule)

        with pytest.raises(ValueError, match="'rule_type' and 'payload'"):
            builder.build()

    def test_when_expanded_composite_creates_duplicate_names_then_raises_duplicate_error(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)

        regular_rule = AliasingRule("rule_sub_0", "prefix_suffix", "Regular", {"prefix": "A"})

        composite_rule = AliasingRule(
            name="rule",
            rule_type="composite",
            description="Composite",
            payload={
                "rules": [
                    {"rule_type": "rule_type1", "payload": {"key": "val"}},
                ]
            },
        )

        builder.with_rule(regular_rule).with_rule(composite_rule)

        with pytest.raises(DuplicateRuleNameError):
            builder.build()

    def test_when_created_with_both_custom_dependencies_then_uses_both(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)

        builder = DefaultAliasingKuiperBuilder(registry=mock_registry, composer=mock_composer)

        assert builder._registry is mock_registry
        assert builder._composer is mock_composer
