import re

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composer import (
    DefaultExpressionComposer,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composition_config import (
    AliasingCompositionConfig,
    FieldMapping,
    OutputProjectionConfig,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.json_path import JSONPath
from cognite_toolkit._cdf_tk.commands.entity_matching.common.macro import Macro


def normalize_expression(expression: str) -> str:
    expression = re.sub(r"v_[0-9a-f]{32}", "v_*", expression)
    expression = re.sub(r"m_[0-9a-f]{32}", "m_*", expression)
    return expression


class TestDefaultExpressionComposer:
    def test_when_created_with_no_config_then_uses_default_config(self) -> None:
        composer = DefaultExpressionComposer()
        assert composer._config is not None
        assert composer._config.keys_path == JSONPath("input.keys")

    def test_when_created_with_custom_config_then_uses_provided_config(self) -> None:
        custom_config = AliasingCompositionConfig(keys_path=JSONPath("custom.path"))
        composer = DefaultExpressionComposer(custom_config)
        assert composer._config == custom_config

    def test_when_compose_with_empty_macros_list_then_returns_empty_expression(self) -> None:
        composer = DefaultExpressionComposer()
        result = composer.compose([])

        expected = 'input.keys.map(entity => {"space": entity.space, "external_id": entity.external_id, "keys": entity.keys, "aliases": []})'
        assert result == expected

    def test_when_compose_with_none_macros_then_returns_empty_expression(self) -> None:
        composer = DefaultExpressionComposer()
        result = composer.compose(None) if None else composer.compose([])

        expected = 'input.keys.map(entity => {"space": entity.space, "external_id": entity.external_id, "keys": entity.keys, "aliases": []})'
        assert result == expected

    def test_when_compose_with_single_macro_then_returns_full_expression(self) -> None:
        composer = DefaultExpressionComposer()
        macro = Macro(definition="(s) => s.replace('a', 'b')")
        result = composer.compose([macro])

        normalized = normalize_expression(result)
        assert "m_* := (s) => s.replace('a', 'b');" in normalized
        assert "#composite_aliases := (v_*) => [m_*(v_*)].flatmap" in normalized
        assert 'input.keys.map(entity => {"space": entity.space' in result

    def test_when_compose_with_multiple_macros_then_chains_all_macros_in_order(self) -> None:
        composer = DefaultExpressionComposer()
        macro1 = Macro(definition="(s) => s.replace('a', 'b')")
        macro2 = Macro(definition="(s) => s.replace('c', 'd')")
        result = composer.compose([macro1, macro2])

        normalized = normalize_expression(result)
        assert "m_* := (s) => s.replace('a', 'b');" in normalized
        assert "m_* := (s) => s.replace('c', 'd');" in normalized
        assert "#composite_aliases := (v_*) => [m_*(v_*), m_*(v_*)].flatmap" in normalized

    def test_when_compose_with_three_macros_then_includes_all_in_composite(self) -> None:
        composer = DefaultExpressionComposer()
        macro1 = Macro(definition="(s) => s.replace('a', 'b')")
        macro2 = Macro(definition="(s) => s.replace('c', 'd')")
        macro3 = Macro(definition="(s) => s.replace('e', 'f')")
        result = composer.compose([macro1, macro2, macro3])

        normalized = normalize_expression(result)
        assert "m_* := (s) => s.replace('a', 'b');" in normalized
        assert "m_* := (s) => s.replace('c', 'd');" in normalized
        assert "m_* := (s) => s.replace('e', 'f');" in normalized
        assert "#composite_aliases := (v_*) => [m_*(v_*), m_*(v_*), m_*(v_*)].flatmap" in normalized

    def test_when_compose_with_custom_keys_path_then_uses_custom_path_in_expression(self) -> None:
        custom_config = AliasingCompositionConfig(keys_path=JSONPath("data.entities"))
        composer = DefaultExpressionComposer(custom_config)
        macro = Macro(definition="(s) => s")
        result = composer.compose([macro])

        assert "data.entities.map(entity =>" in result
        assert "#composite_aliases := (v_" in result

    def test_when_compose_with_custom_output_projection_then_uses_custom_fields(self) -> None:
        custom_projection = OutputProjectionConfig(
            aliasing_output_name="custom_aliases",
            fields=[
                FieldMapping("id", "external_id"),
                FieldMapping("space_name", "space"),
            ],
        )
        custom_config = AliasingCompositionConfig(output_projection=custom_projection)
        composer = DefaultExpressionComposer(custom_config)
        macro = Macro(definition="(s) => s")
        result = composer.compose([macro])

        normalized = normalize_expression(result)
        assert "#composite_aliases := (v_*) => [m_*(v_*)].flatmap" in normalized
        assert '"id": entity.external_id' in result
        assert '"space_name": entity.space' in result
        assert '"custom_aliases": composite_aliases' in result

    def test_when_build_empty_expression_then_returns_correct_format(self) -> None:
        composer = DefaultExpressionComposer()
        result = composer._build_empty_expression()

        expected = 'input.keys.map(entity => {"space": entity.space, "external_id": entity.external_id, "keys": entity.keys, "aliases": []})'
        assert result == expected

    def test_when_build_empty_expression_with_custom_keys_path_then_uses_custom_path(self) -> None:
        custom_config = AliasingCompositionConfig(keys_path=JSONPath("custom.path"))
        composer = DefaultExpressionComposer(custom_config)
        result = composer._build_empty_expression()

        expected = 'custom.path.map(entity => {"space": entity.space, "external_id": entity.external_id, "keys": entity.keys, "aliases": []})'
        assert result == expected
