from unittest.mock import Mock, call

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.api.facade import AliasingFacade
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper import AliasingKuiper
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import (
    AliasingKuiperBuilder,
    AliasingKuiperBuilderFactory,
    AliasingRule,
)


class TestAliasingFacade:
    def test_when_generate_with_single_rule_then_factory_creates_builder_and_builds(self) -> None:
        factory = Mock(spec=AliasingKuiperBuilderFactory)
        builder = Mock(spec=AliasingKuiperBuilder)
        built_kuiper = AliasingKuiper(expression="test_expression")
        builder.build.return_value = built_kuiper
        builder.with_rule.return_value = builder
        factory.create.return_value = builder

        rule = AliasingRule(
            name="test_rule",
            rule_type="character_substitution",
            description="A test rule",
            payload={"from": "a", "to": "b"},
        )
        facade = AliasingFacade(factory)

        result = facade.generate([rule])

        factory.create.assert_called_once()
        builder.with_rule.assert_called_once_with(rule)
        builder.build.assert_called_once()
        assert result == built_kuiper

    def test_when_generate_with_multiple_rules_then_builder_receives_all_rules_in_order(self) -> None:
        factory = Mock(spec=AliasingKuiperBuilderFactory)
        builder = Mock(spec=AliasingKuiperBuilder)
        built_kuiper = AliasingKuiper(expression="combined_expression")
        builder.build.return_value = built_kuiper
        builder.with_rule.return_value = builder
        factory.create.return_value = builder

        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="First rule",
            payload={"from": "a", "to": "b"},
        )
        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Second rule",
            payload={"from": "c", "to": "d"},
        )
        facade = AliasingFacade(factory)

        result = facade.generate([rule1, rule2])

        factory.create.assert_called_once()
        expected_calls = [call(rule1), call(rule2)]
        builder.with_rule.assert_has_calls(expected_calls)
        assert builder.with_rule.call_count == 2
        builder.build.assert_called_once()
        assert result == built_kuiper

    def test_when_generate_with_empty_rules_list_then_raises_value_error(self) -> None:
        factory = Mock(spec=AliasingKuiperBuilderFactory)
        facade = AliasingFacade(factory)

        with pytest.raises(ValueError, match="At least one rule must be provided"):
            facade.generate([])

    def test_when_generate_with_empty_rules_list_then_factory_never_called(self) -> None:
        factory = Mock(spec=AliasingKuiperBuilderFactory)
        facade = AliasingFacade(factory)

        with pytest.raises(ValueError):
            facade.generate([])

        factory.create.assert_not_called()

    def test_when_generate_returns_kuiper_with_correct_type(self) -> None:
        factory = Mock(spec=AliasingKuiperBuilderFactory)
        builder = Mock(spec=AliasingKuiperBuilder)
        built_kuiper = AliasingKuiper(expression="(s) => s.replace('a', 'b')")
        builder.build.return_value = built_kuiper
        builder.with_rule.return_value = builder
        factory.create.return_value = builder

        rule = AliasingRule(
            name="test_rule",
            rule_type="character_substitution",
            description="A test rule",
            payload={"from": "a", "to": "b"},
        )
        facade = AliasingFacade(factory)

        result = facade.generate([rule])

        assert isinstance(result, AliasingKuiper)
        assert result.expression == "(s) => s.replace('a', 'b')"

    def test_when_generate_called_multiple_times_then_factory_creates_fresh_builder_each_call(self) -> None:
        factory = Mock(spec=AliasingKuiperBuilderFactory)
        builder1 = Mock(spec=AliasingKuiperBuilder)
        builder2 = Mock(spec=AliasingKuiperBuilder)
        kuiper1 = AliasingKuiper(expression="expression1")
        kuiper2 = AliasingKuiper(expression="expression2")
        builder1.build.return_value = kuiper1
        builder1.with_rule.return_value = builder1
        builder2.build.return_value = kuiper2
        builder2.with_rule.return_value = builder2
        factory.create.side_effect = [builder1, builder2]

        rule1 = AliasingRule(
            name="rule1",
            rule_type="character_substitution",
            description="Rule 1",
            payload={"from": "a", "to": "b"},
        )
        rule2 = AliasingRule(
            name="rule2",
            rule_type="character_substitution",
            description="Rule 2",
            payload={"from": "c", "to": "d"},
        )
        facade = AliasingFacade(factory)

        result1 = facade.generate([rule1])
        result2 = facade.generate([rule2])

        assert factory.create.call_count == 2
        assert result1 == kuiper1
        assert result2 == kuiper2
