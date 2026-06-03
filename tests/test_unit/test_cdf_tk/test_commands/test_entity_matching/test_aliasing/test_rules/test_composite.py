from unittest.mock import Mock

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.composite import (
    CompositeRuleContext,
    CompositeRuleDefinition,
    ResolvedRuleSpec,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro


class TestResolvedRuleSpec:
    def test_when_valid_resolved_spec_then_creation_succeeds(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        spec = ResolvedRuleSpec(definition=mock_definition, payload={"prefix": "test"})
        assert spec.definition is mock_definition
        assert spec.payload == {"prefix": "test"}


class TestCompositeRuleContext:
    def test_when_valid_rules_then_creation_succeeds(self) -> None:
        mock_def1 = Mock(spec=RuleDefinition)
        mock_def2 = Mock(spec=RuleDefinition)
        rule1 = ResolvedRuleSpec(definition=mock_def1, payload={"prefix": "P"})
        rule2 = ResolvedRuleSpec(definition=mock_def2, payload={"case": "upper"})
        context = CompositeRuleContext(rules=[rule1, rule2])

        assert len(context.rules) == 2
        assert context.rules[0].definition is mock_def1
        assert context.rules[1].definition is mock_def2

    def test_when_single_rule_then_creation_succeeds(self) -> None:
        mock_def = Mock(spec=RuleDefinition)
        rule = ResolvedRuleSpec(definition=mock_def, payload={"prefix": "P"})
        context = CompositeRuleContext(rules=[rule])

        assert len(context.rules) == 1
        assert context.rules[0].definition is mock_def

    def test_when_empty_rules_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="At least one rule must be specified"):
            CompositeRuleContext(rules=[])

    def test_when_non_resolved_spec_item_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="All items in rules list must be ResolvedRuleSpec instances"):
            CompositeRuleContext(rules=[{"definition": Mock()}])  # type: ignore[list-item]


class TestCompositeRuleDefinition:
    def test_when_calling_type_then_returns_composite_rule_type(self) -> None:
        rule_def = CompositeRuleDefinition()

        result = rule_def.type()

        assert result == RuleType.COMPOSITE

    def test_when_deserialize_with_valid_single_rule_then_context_created(self) -> None:
        mock_definition = Mock(spec=RuleDefinition)
        resolved_spec = ResolvedRuleSpec(definition=mock_definition, payload={"prefix": "P"})

        rule_def = CompositeRuleDefinition()
        payload = {"rules": [resolved_spec]}

        context = rule_def.deserialize_context(payload)

        assert len(context.rules) == 1
        assert context.rules[0].definition is mock_definition

    def test_when_deserialize_with_multiple_rules_then_all_deserialized(self) -> None:
        mock_def1 = Mock(spec=RuleDefinition)
        mock_def2 = Mock(spec=RuleDefinition)
        resolved_spec1 = ResolvedRuleSpec(definition=mock_def1, payload={"prefix": "P"})
        resolved_spec2 = ResolvedRuleSpec(definition=mock_def2, payload={"case": "upper"})

        rule_def = CompositeRuleDefinition()
        payload = {"rules": [resolved_spec1, resolved_spec2]}

        context = rule_def.deserialize_context(payload)

        assert len(context.rules) == 2
        assert context.rules[0].definition is mock_def1
        assert context.rules[1].definition is mock_def2

    def test_when_deserialize_missing_rules_key_then_raises_value_error(self) -> None:
        rule_def = CompositeRuleDefinition()
        payload: dict[str, list[str]] = {"other_key": []}

        with pytest.raises(ValueError, match="Composite rule payload must contain 'rules' key"):
            rule_def.deserialize_context(payload)

    def test_when_deserialize_rules_not_list_then_raises_value_error(self) -> None:
        rule_def = CompositeRuleDefinition()
        payload = {"rules": "not_a_list"}

        with pytest.raises(ValueError, match="'rules' must be a list"):
            rule_def.deserialize_context(payload)

    def test_when_deserialize_empty_rules_then_raises_value_error(self) -> None:
        rule_def = CompositeRuleDefinition()
        payload: dict[str, list[str]] = {"rules": []}

        with pytest.raises(ValueError, match="'rules' list cannot be empty"):
            rule_def.deserialize_context(payload)

    def test_when_deserialize_non_resolved_spec_item_then_raises_value_error(self) -> None:
        rule_def = CompositeRuleDefinition()
        payload = {"rules": [{"definition": Mock()}]}

        with pytest.raises(ValueError, match="All items in rules must be ResolvedRuleSpec instances"):
            rule_def.deserialize_context(payload)

    def test_when_create_macro_with_single_sub_rule_then_generates_valid_macro(self) -> None:
        mock_sub_macro = Macro(definition="(m) => m.map(v => v + '_SUFFIX')")
        mock_sub_definition = Mock(spec=RuleDefinition)
        mock_sub_definition.deserialize_context.return_value = Mock()
        mock_sub_definition.create_kuiper_macro.return_value = mock_sub_macro

        context = CompositeRuleContext(
            rules=[ResolvedRuleSpec(definition=mock_sub_definition, payload={"prefix": "P"})]
        )

        rule_def = CompositeRuleDefinition()

        macro = rule_def.create_kuiper_macro(context)

        assert isinstance(macro, Macro)
        assert macro.definition is not None
        assert len(macro.definition) > 0
        assert "=>" in macro.definition

    def test_when_create_macro_with_two_sub_rules_then_chains_sequentially(self) -> None:
        mock_macro1 = Macro(definition="(m) => m.map(v => v + '_1')")
        mock_macro2 = Macro(definition="(m) => m.map(v => v + '_2')")

        mock_definition1 = Mock(spec=RuleDefinition)
        mock_definition1.deserialize_context.return_value = Mock()
        mock_definition1.create_kuiper_macro.return_value = mock_macro1

        mock_definition2 = Mock(spec=RuleDefinition)
        mock_definition2.deserialize_context.return_value = Mock()
        mock_definition2.create_kuiper_macro.return_value = mock_macro2

        context = CompositeRuleContext(
            rules=[
                ResolvedRuleSpec(definition=mock_definition1, payload={"key1": "val1"}),
                ResolvedRuleSpec(definition=mock_definition2, payload={"key2": "val2"}),
            ]
        )

        rule_def = CompositeRuleDefinition()
        macro = rule_def.create_kuiper_macro(context)

        assert isinstance(macro, Macro)
        assert macro.definition is not None
        assert "map" in macro.definition
        assert "=>" in macro.definition

    def test_when_create_macro_with_three_sub_rules_then_all_included(self) -> None:
        mock_macros = [
            Macro(definition="(m) => m.map(v => v + '_1')"),
            Macro(definition="(m) => m.map(v => v + '_2')"),
            Macro(definition="(m) => m.map(v => v + '_3')"),
        ]

        mock_definitions = []
        for macro in mock_macros:
            mock_def = Mock(spec=RuleDefinition)
            mock_def.deserialize_context.return_value = Mock()
            mock_def.create_kuiper_macro.return_value = macro
            mock_definitions.append(mock_def)

        context = CompositeRuleContext(
            rules=[
                ResolvedRuleSpec(definition=mock_definitions[0], payload={"key": "val"}),
                ResolvedRuleSpec(definition=mock_definitions[1], payload={"key": "val"}),
                ResolvedRuleSpec(definition=mock_definitions[2], payload={"key": "val"}),
            ]
        )

        rule_def = CompositeRuleDefinition()
        macro = rule_def.create_kuiper_macro(context)

        assert isinstance(macro, Macro)
        assert macro.definition is not None
        assert "=>" in macro.definition
