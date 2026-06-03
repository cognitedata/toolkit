from collections.abc import Callable

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.api.facade import AliasingFacade
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import (
    AliasingKuiperBuilder,
    DefaultAliasingKuiperBuilder,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composer import (
    DefaultExpressionComposer,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composition_config import (
    AliasingCompositionConfig,
    OutputProjectionConfig,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.registry import LocalRuleDefinitionRegistry
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.registry.rules_discovery import LocalRulesDiscovery
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


def provide_aliasing_facade(
    builder_provider: Callable[[], AliasingKuiperBuilder] | None = None,
) -> AliasingFacade:
    if builder_provider is None:
        registry = LocalRuleDefinitionRegistry.bootstrap(LocalRulesDiscovery.create())
        composer = DefaultExpressionComposer(provide_aliasing_composition_config())
        resolved_builder_provider = lambda: DefaultAliasingKuiperBuilder(registry=registry, composer=composer)
    else:
        resolved_builder_provider = builder_provider
    return AliasingFacade(resolved_builder_provider)
