from unittest.mock import Mock

import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.api.facade import AliasingFacade
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import (
    AliasingKuiperBuilderFactory,
    AliasingRule,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composition_config import (
    AliasingCompositionConfig,
    OutputProjectionConfig,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.bootstrap.bootstrapper import (
    provide_aliasing_composition_config,
    provide_aliasing_facade,
    provide_json_path,
    provide_output_projection_config,
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


class TestProvideAliasingFacade:
    def test_with_no_args_returns_facade(self) -> None:
        result = provide_aliasing_facade()
        assert isinstance(result, AliasingFacade)

    def test_with_custom_factory_uses_provided_factory(self) -> None:
        mock_factory = Mock(spec=AliasingKuiperBuilderFactory)
        result = provide_aliasing_facade(factory=mock_factory)
        assert result._factory is mock_factory

    def test_default_facade_generates_expression_for_known_rule(self) -> None:
        facade = provide_aliasing_facade()
        kuiper = facade.generate(
            [
                AliasingRule(
                    name="test_rule",
                    rule_type="character_substitution",
                    description="Test rule",
                    payload={"replacements": {"P": "K"}},
                )
            ]
        )

        assert kuiper.expression
        assert "map(entity =>" in kuiper.expression
