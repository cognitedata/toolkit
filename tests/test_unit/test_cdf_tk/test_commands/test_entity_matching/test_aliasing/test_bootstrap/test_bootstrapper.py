from unittest.mock import Mock

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.api.facade import AliasingFacade
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import (
    AliasingKuiperBuilder,
    AliasingKuiperBuilderFactory,
    DefaultAliasingKuiperBuilder,
    DefaultAliasingKuiperBuilderFactory,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composer import (
    DefaultExpressionComposer,
    ExpressionComposer,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composition_config import (
    AliasingCompositionConfig,
    OutputProjectionConfig,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.bootstrap.bootstrapper import (
    provide_aliasing_composition_config,
    provide_aliasing_facade,
    provide_aliasing_kuiper_builder,
    provide_aliasing_kuiper_builder_factory,
    provide_expression_composer,
    provide_json_path,
    provide_output_projection_config,
    provide_rule_definition_registry,
    provide_rules_discovery,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.registry import (
    LocalRuleDefinitionRegistry,
    RuleDefinitionRegistry,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.rules_discovery import (
    LocalRulesDiscovery,
    RulesDiscovery,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.json_path import InvalidJSONPathError, JSONPath


class TestProvideJsonPath:
    def test_with_no_args_returns_default_path(self) -> None:
        result = provide_json_path()
        assert isinstance(result, JSONPath)
        assert str(result) == "input.keys"

    def test_with_custom_path_returns_provided_path(self) -> None:
        custom_path = "data.records"
        result = provide_json_path(custom_path)
        assert isinstance(result, JSONPath)
        assert str(result) == custom_path

    def test_with_invalid_path_raises_error(self) -> None:
        with pytest.raises(InvalidJSONPathError):
            provide_json_path(".invalid")

    def test_each_call_creates_new_instance(self) -> None:
        result1 = provide_json_path()
        result2 = provide_json_path()
        assert result1 is not result2
        assert result1 == result2


class TestProvideOutputProjectionConfig:
    def test_returns_default_output_projection(self) -> None:
        result = provide_output_projection_config()
        assert isinstance(result, OutputProjectionConfig)
        assert result.aliasing_output_name == "aliases"

    def test_default_includes_standard_fields(self) -> None:
        result = provide_output_projection_config()
        field_names = [f.output_field_name for f in result.fields]
        assert "space" in field_names
        assert "external_id" in field_names
        assert "keys" in field_names

    def test_each_call_creates_new_instance(self) -> None:
        result1 = provide_output_projection_config()
        result2 = provide_output_projection_config()
        assert result1 is not result2
        assert result1 == result2


class TestProvideAliasingCompositionConfig:
    def test_with_no_args_returns_config_with_defaults(self) -> None:
        result = provide_aliasing_composition_config()
        assert isinstance(result, AliasingCompositionConfig)
        assert str(result.keys_path) == "input.keys"
        assert result.output_projection is not None

    def test_with_custom_keys_path_uses_provided_path(self) -> None:
        custom_path = provide_json_path("custom.data")
        result = provide_aliasing_composition_config(keys_path=custom_path)
        assert str(result.keys_path) == "custom.data"

    def test_with_custom_projection_uses_provided_projection(self) -> None:
        custom_projection = OutputProjectionConfig.default()
        result = provide_aliasing_composition_config(output_projection=custom_projection)
        assert result.output_projection is custom_projection

    def test_with_both_custom_args_uses_all(self) -> None:
        custom_path = provide_json_path("my.path")
        custom_projection = OutputProjectionConfig.default()
        result = provide_aliasing_composition_config(
            keys_path=custom_path,
            output_projection=custom_projection,
        )
        assert str(result.keys_path) == "my.path"
        assert result.output_projection is custom_projection

    def test_each_call_creates_new_instance(self) -> None:
        result1 = provide_aliasing_composition_config()
        result2 = provide_aliasing_composition_config()
        assert result1 is not result2


class TestProvideRulesDiscovery:
    def test_returns_rules_discovery_interface(self) -> None:
        result = provide_rules_discovery()
        assert isinstance(result, RulesDiscovery)

    def test_returns_local_rules_discovery_implementation(self) -> None:
        result = provide_rules_discovery()
        assert isinstance(result, LocalRulesDiscovery)

    def test_each_call_creates_new_instance(self) -> None:
        result1 = provide_rules_discovery()
        result2 = provide_rules_discovery()
        assert result1 is not result2

    def test_discovery_can_discover_rules(self) -> None:
        result = provide_rules_discovery()
        discovered = result.discover_rules()
        assert isinstance(discovered, dict)


class TestProvideRuleDefinitionRegistry:
    def test_with_no_args_returns_registry_with_discovered_rules(self) -> None:
        result = provide_rule_definition_registry()
        assert isinstance(result, RuleDefinitionRegistry)
        assert isinstance(result, LocalRuleDefinitionRegistry)

    def test_with_custom_discovery_uses_provided_discovery(self) -> None:
        mock_discovery = Mock(spec=RulesDiscovery)
        mock_discovery.discover_rules.return_value = {}

        result = provide_rule_definition_registry(discovery=mock_discovery)
        assert isinstance(result, RuleDefinitionRegistry)
        mock_discovery.discover_rules.assert_called_once()

    def test_default_registry_has_character_substitution_rule(self) -> None:
        result = provide_rule_definition_registry()
        rules = result.get_definition_or_throw("character_substitution")  # type: ignore[arg-type]
        assert rules is not None

    def test_each_call_creates_new_instance(self) -> None:
        result1 = provide_rule_definition_registry()
        result2 = provide_rule_definition_registry()
        assert result1 is not result2

    def test_with_custom_discovery_each_call_creates_new_registry(self) -> None:
        mock_discovery = Mock(spec=RulesDiscovery)
        mock_discovery.discover_rules.return_value = {}

        result1 = provide_rule_definition_registry(discovery=mock_discovery)
        result2 = provide_rule_definition_registry(discovery=mock_discovery)
        assert result1 is not result2


class TestProvideExpressionComposer:
    def test_with_no_args_returns_composer_with_defaults(self) -> None:
        result = provide_expression_composer()
        assert isinstance(result, ExpressionComposer)
        assert isinstance(result, DefaultExpressionComposer)

    def test_with_custom_config_uses_provided_config(self) -> None:
        custom_config = provide_aliasing_composition_config()
        result = provide_expression_composer(config=custom_config)
        assert isinstance(result, ExpressionComposer)
        assert result._config is custom_config  # type: ignore[attr-defined]

    def test_default_composer_has_composition_config(self) -> None:
        result = provide_expression_composer()
        assert result._config is not None  # type: ignore[attr-defined]
        assert isinstance(result._config, AliasingCompositionConfig)  # type: ignore[attr-defined]

    def test_each_call_creates_new_instance(self) -> None:
        result1 = provide_expression_composer()
        result2 = provide_expression_composer()
        assert result1 is not result2


class TestProvideAliasingKuiperBuilder:
    def test_with_no_args_returns_builder_with_auto_resolved_deps(self) -> None:
        result = provide_aliasing_kuiper_builder()
        assert isinstance(result, AliasingKuiperBuilder)
        assert isinstance(result, DefaultAliasingKuiperBuilder)

    def test_with_custom_registry_uses_provided_registry(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        result = provide_aliasing_kuiper_builder(registry=mock_registry)
        assert result._registry is mock_registry  # type: ignore[attr-defined]

    def test_with_custom_composer_uses_provided_composer(self) -> None:
        mock_composer = Mock(spec=ExpressionComposer)
        result = provide_aliasing_kuiper_builder(composer=mock_composer)
        assert result._composer is mock_composer  # type: ignore[attr-defined]

    def test_with_both_custom_deps_uses_all(self) -> None:
        mock_registry = Mock(spec=RuleDefinitionRegistry)
        mock_composer = Mock(spec=ExpressionComposer)
        result = provide_aliasing_kuiper_builder(
            registry=mock_registry,
            composer=mock_composer,
        )
        assert result._registry is mock_registry  # type: ignore[attr-defined]
        assert result._composer is mock_composer  # type: ignore[attr-defined]

    def test_default_builder_resolves_all_dependencies(self) -> None:
        result = provide_aliasing_kuiper_builder()
        assert result._registry is not None  # type: ignore[attr-defined]
        assert result._composer is not None  # type: ignore[attr-defined]
        assert isinstance(result._registry, RuleDefinitionRegistry)  # type: ignore[attr-defined]
        assert isinstance(result._composer, ExpressionComposer)  # type: ignore[attr-defined]

    def test_each_call_creates_new_instance(self) -> None:
        result1 = provide_aliasing_kuiper_builder()
        result2 = provide_aliasing_kuiper_builder()
        assert result1 is not result2

    def test_each_call_creates_new_dependencies(self) -> None:
        result1 = provide_aliasing_kuiper_builder()
        result2 = provide_aliasing_kuiper_builder()
        assert result1._registry is not result2._registry  # type: ignore[attr-defined]
        assert result1._composer is not result2._composer  # type: ignore[attr-defined]


class TestProvideAliasingFacade:
    def test_with_no_args_returns_facade_with_auto_resolved_factory(self) -> None:
        result = provide_aliasing_facade()
        assert isinstance(result, AliasingFacade)

    def test_with_custom_factory_uses_provided_factory(self) -> None:
        mock_factory = Mock(spec=AliasingKuiperBuilderFactory)
        result = provide_aliasing_facade(factory=mock_factory)
        assert result._factory is mock_factory

    def test_default_facade_has_factory(self) -> None:
        result = provide_aliasing_facade()
        assert result._factory is not None
        assert isinstance(result._factory, AliasingKuiperBuilderFactory)

    def test_each_call_creates_new_instance(self) -> None:
        result1 = provide_aliasing_facade()
        result2 = provide_aliasing_facade()
        assert result1 is not result2

    def test_each_call_creates_new_factory(self) -> None:
        result1 = provide_aliasing_facade()
        result2 = provide_aliasing_facade()
        assert result1._factory is not result2._factory


class TestBootstrapperDependencyComposition:
    def test_full_dependency_chain_with_defaults(self) -> None:
        facade = provide_aliasing_facade()
        assert isinstance(facade, AliasingFacade)
        assert isinstance(facade._factory, AliasingKuiperBuilderFactory)
        assert isinstance(facade._factory, DefaultAliasingKuiperBuilderFactory)

    def test_override_at_factory_level_propagates_to_facade(self) -> None:
        custom_composer = Mock(spec=ExpressionComposer)
        custom_registry = Mock(spec=RuleDefinitionRegistry)
        custom_factory = DefaultAliasingKuiperBuilderFactory(
            registry=custom_registry,
            composer=custom_composer,
        )
        facade = provide_aliasing_facade(factory=custom_factory)

        assert facade._factory is custom_factory
        builder = facade._factory.create()
        assert builder._composer is custom_composer  # type: ignore[attr-defined]
        assert builder._registry is custom_registry  # type: ignore[attr-defined]

    def test_multiple_facade_instances_are_independent(self) -> None:
        facade1 = provide_aliasing_facade()
        facade2 = provide_aliasing_facade()

        assert facade1 is not facade2
        assert facade1._factory is not facade2._factory

    def test_factory_creates_fresh_builders(self) -> None:
        factory = provide_aliasing_kuiper_builder_factory()
        builder1 = factory.create()
        builder2 = factory.create()

        assert builder1 is not builder2
        assert isinstance(builder1, DefaultAliasingKuiperBuilder)
        assert isinstance(builder2, DefaultAliasingKuiperBuilder)

    def test_all_factories_return_interfaces_not_implementation_types(self) -> None:
        json_path = provide_json_path()
        assert type(json_path).__name__ == "JSONPath"

        discovery = provide_rules_discovery()
        assert isinstance(discovery, RulesDiscovery)

        registry = provide_rule_definition_registry()
        assert isinstance(registry, RuleDefinitionRegistry)

        composer = provide_expression_composer()
        assert isinstance(composer, ExpressionComposer)

        builder = provide_aliasing_kuiper_builder()
        assert isinstance(builder, AliasingKuiperBuilder)

        factory = provide_aliasing_kuiper_builder_factory()
        assert isinstance(factory, AliasingKuiperBuilderFactory)

        facade = provide_aliasing_facade()
        assert isinstance(facade, AliasingFacade)
