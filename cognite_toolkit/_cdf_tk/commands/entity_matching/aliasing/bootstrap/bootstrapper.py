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
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.registry import (
    LocalRuleDefinitionRegistry,
    RuleDefinitionRegistry,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.rules_discovery import (
    LocalRulesDiscovery,
    RulesDiscovery,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.json_path import JSONPath


def provide_json_path(path: str | None = None) -> JSONPath:
    return JSONPath(path or "input.keys")


def provide_output_projection_config() -> OutputProjectionConfig:
    return OutputProjectionConfig.default()


def provide_aliasing_composition_config(
    keys_path: JSONPath | None = None,
    output_projection: OutputProjectionConfig | None = None,
) -> AliasingCompositionConfig:
    resolved_keys_path = keys_path or provide_json_path()
    resolved_projection = output_projection or provide_output_projection_config()

    return AliasingCompositionConfig(
        keys_path=resolved_keys_path,
        output_projection=resolved_projection,
    )


def provide_rules_discovery() -> RulesDiscovery:
    return LocalRulesDiscovery.create()


def provide_rule_definition_registry(
    discovery: RulesDiscovery | None = None,
) -> RuleDefinitionRegistry:
    resolved_discovery = discovery or provide_rules_discovery()
    return LocalRuleDefinitionRegistry.bootstrap(resolved_discovery)


def provide_expression_composer(
    config: AliasingCompositionConfig | None = None,
) -> ExpressionComposer:
    resolved_config = config or provide_aliasing_composition_config()
    return DefaultExpressionComposer(resolved_config)


def provide_aliasing_kuiper_builder(
    registry: RuleDefinitionRegistry | None = None,
    composer: ExpressionComposer | None = None,
) -> AliasingKuiperBuilder:
    resolved_registry = registry or provide_rule_definition_registry()
    resolved_composer = composer or provide_expression_composer()

    return DefaultAliasingKuiperBuilder(
        registry=resolved_registry,
        composer=resolved_composer,
    )


def provide_aliasing_kuiper_builder_factory(
    registry: RuleDefinitionRegistry | None = None,
    composer: ExpressionComposer | None = None,
) -> AliasingKuiperBuilderFactory:
    resolved_registry = registry or provide_rule_definition_registry()
    resolved_composer = composer or provide_expression_composer()

    return DefaultAliasingKuiperBuilderFactory(
        registry=resolved_registry,
        composer=resolved_composer,
    )


def provide_aliasing_facade(
    factory: AliasingKuiperBuilderFactory | None = None,
) -> AliasingFacade:
    resolved_factory = factory or provide_aliasing_kuiper_builder_factory()
    return AliasingFacade(resolved_factory)
