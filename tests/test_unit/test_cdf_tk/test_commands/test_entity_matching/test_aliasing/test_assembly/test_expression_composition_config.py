import pytest

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.expression_composition_config import (
    AliasingCompositionConfig,
    AliasingCompositionConfigBuilder,
    FieldMapping,
    OutputProjectionConfig,
    OutputProjectionConfigBuilder,
)
from cognite_toolkit._cdf_tk.commands.entity_matching.common.json_path import JSONPath


class TestFieldMapping:
    def test_when_valid_output_field_name_and_path_then_creation_succeeds(self) -> None:
        mapping = FieldMapping("external_id", "id")
        assert mapping.output_field_name == "external_id"
        assert mapping.field_path == "id"

    def test_when_single_field_then_creation_succeeds(self) -> None:
        mapping = FieldMapping("space", "space")
        assert mapping.output_field_name == "space"
        assert mapping.field_path == "space"

    def test_when_empty_output_field_name_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="output_field_name cannot be empty"):
            FieldMapping("", "path")

    def test_when_empty_field_path_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="field_path cannot be empty"):
            FieldMapping("name", "")

    def test_when_none_output_field_name_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="output_field_name cannot be empty"):
            FieldMapping(None, "path")  # type: ignore[arg-type]

    def test_when_none_field_path_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="field_path cannot be empty"):
            FieldMapping("name", None)  # type: ignore[arg-type]

    def test_when_one_to_one_factory_then_creates_mapping_with_same_values(self) -> None:
        mapping = FieldMapping.one_to_one("space")
        assert mapping.output_field_name == "space"
        assert mapping.field_path == "space"

    def test_when_one_to_one_with_different_names_then_creates_mapping_correctly(self) -> None:
        mapping = FieldMapping.one_to_one("custom_field")
        assert mapping.output_field_name == "custom_field"
        assert mapping.field_path == "custom_field"

    def test_when_field_mapping_is_frozen_then_cannot_be_modified(self) -> None:
        mapping = FieldMapping("field", "path")
        with pytest.raises(AttributeError):
            mapping.output_field_name = "new_field"  # type: ignore[misc]

    def test_when_two_identical_mappings_then_are_equal(self) -> None:
        mapping1 = FieldMapping("external_id", "id")
        mapping2 = FieldMapping("external_id", "id")
        assert mapping1 == mapping2


class TestOutputProjectionConfig:
    def test_when_valid_aliasing_output_name_and_fields_then_creation_succeeds(self) -> None:
        fields = [FieldMapping.one_to_one("space")]
        config = OutputProjectionConfig("aliases", fields)
        assert config.aliasing_output_name == "aliases"
        assert config.fields == fields

    def test_when_multiple_fields_then_creation_succeeds(self) -> None:
        fields = [
            FieldMapping.one_to_one("space"),
            FieldMapping.one_to_one("external_id"),
        ]
        config = OutputProjectionConfig("aliases", fields)
        assert config.aliasing_output_name == "aliases"
        assert len(config.fields) == 2

    def test_when_empty_aliasing_output_name_then_raises_value_error(self) -> None:
        fields = [FieldMapping.one_to_one("space")]
        with pytest.raises(ValueError, match="aliasing_output_name cannot be empty"):
            OutputProjectionConfig("", fields)

    def test_when_none_aliasing_output_name_then_raises_value_error(self) -> None:
        fields = [FieldMapping.one_to_one("space")]
        with pytest.raises(ValueError, match="aliasing_output_name cannot be empty"):
            OutputProjectionConfig(None, fields)  # type: ignore[arg-type]

    def test_when_empty_fields_list_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="fields list cannot be empty"):
            OutputProjectionConfig("aliases", [])

    def test_when_none_fields_list_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="fields list cannot be empty"):
            OutputProjectionConfig("aliases", None)  # type: ignore[arg-type]

    def test_when_generate_projection_object_with_single_field_then_returns_correct_json_object(self) -> None:
        config = OutputProjectionConfig("aliases", [FieldMapping("external_id", "id")])
        result = config.generate_projection_object("entity", "[]")

        expected = '{"external_id": entity.id, "aliases": []}'
        assert result == expected

    def test_when_generate_projection_object_with_multiple_fields_then_includes_all_fields(self) -> None:
        config = OutputProjectionConfig(
            "aliases",
            [
                FieldMapping("space", "space"),
                FieldMapping("external_id", "id"),
            ],
        )
        result = config.generate_projection_object("entity", "[]")

        expected = '{"space": entity.space, "external_id": entity.id, "aliases": []}'
        assert result == expected

    def test_when_generate_projection_object_with_aliasing_expression_then_includes_expression(self) -> None:
        config = OutputProjectionConfig("aliases", [FieldMapping.one_to_one("space")])
        result = config.generate_projection_object("entity", "composite_aliases(entity.keys)")

        expected = '{"space": entity.space, "aliases": composite_aliases(entity.keys)}'
        assert result == expected

    def test_when_default_factory_then_returns_standard_config(self) -> None:
        config = OutputProjectionConfig.default()

        assert config.aliasing_output_name == "aliases"
        assert len(config.fields) == 3
        assert config.fields[0].output_field_name == "space"
        assert config.fields[1].output_field_name == "external_id"
        assert config.fields[2].output_field_name == "keys"

    def test_when_default_factory_projection_object_generation_then_generates_correct_format(self) -> None:
        config = OutputProjectionConfig.default()
        result = config.generate_projection_object("entity", "[]")

        expected = '{"space": entity.space, "external_id": entity.external_id, "keys": entity.keys, "aliases": []}'
        assert result == expected

    def test_when_output_projection_config_is_frozen_then_cannot_be_modified(self) -> None:
        config = OutputProjectionConfig("aliases", [FieldMapping.one_to_one("space")])
        with pytest.raises(AttributeError):
            config.aliasing_output_name = "new_aliases"  # type: ignore[misc]


class TestOutputProjectionConfigBuilder:
    def test_when_building_with_single_field_then_config_created(self) -> None:
        builder = OutputProjectionConfigBuilder()
        config = builder.with_field_from_entity("space", "space").with_aliasing_output_name("aliases").build()

        assert config.aliasing_output_name == "aliases"
        assert len(config.fields) == 1
        assert config.fields[0].output_field_name == "space"

    def test_when_building_with_multiple_fields_then_all_added_in_order(self) -> None:
        builder = OutputProjectionConfigBuilder()
        config = (
            builder.with_field_from_entity("space", "space")
            .with_field_from_entity("external_id", "id")
            .with_field_from_entity("keys", "keys")
            .with_aliasing_output_name("aliases")
            .build()
        )

        assert len(config.fields) == 3
        assert config.fields[0].output_field_name == "space"
        assert config.fields[1].output_field_name == "external_id"
        assert config.fields[2].output_field_name == "keys"
        assert config.aliasing_output_name == "aliases"

    def test_when_with_field_from_entity_returns_builder_then_fluent_chaining_works(self) -> None:
        builder = OutputProjectionConfigBuilder()
        result = builder.with_field_from_entity("space", "space")
        assert isinstance(result, OutputProjectionConfigBuilder)
        assert result is builder

    def test_when_with_aliasing_output_name_returns_builder_then_fluent_chaining_works(self) -> None:
        builder = OutputProjectionConfigBuilder()
        result = builder.with_aliasing_output_name("aliases")
        assert isinstance(result, OutputProjectionConfigBuilder)
        assert result is builder

    def test_when_empty_output_name_in_with_field_then_raises_value_error(self) -> None:
        builder = OutputProjectionConfigBuilder()
        with pytest.raises(ValueError, match="output_name cannot be empty"):
            builder.with_field_from_entity("", "path")

    def test_when_empty_field_path_in_with_field_then_raises_value_error(self) -> None:
        builder = OutputProjectionConfigBuilder()
        with pytest.raises(ValueError, match="field_path cannot be empty"):
            builder.with_field_from_entity("name", "")

    def test_when_empty_aliasing_output_name_then_raises_value_error(self) -> None:
        builder = OutputProjectionConfigBuilder()
        with pytest.raises(ValueError, match="aliasing_output_name cannot be empty"):
            builder.with_aliasing_output_name("")

    def test_when_building_without_fields_then_raises_value_error(self) -> None:
        builder = OutputProjectionConfigBuilder()
        with pytest.raises(ValueError, match="At least one field must be added before building"):
            builder.with_aliasing_output_name("aliases").build()

    def test_when_building_without_aliasing_output_name_then_raises_value_error(self) -> None:
        builder = OutputProjectionConfigBuilder()
        with pytest.raises(ValueError, match="aliasing_output_name must be set before building"):
            builder.with_field_from_entity("space", "space").build()

    def test_when_building_with_fields_and_name_set_then_succeeds(self) -> None:
        builder = OutputProjectionConfigBuilder()
        config = builder.with_field_from_entity("space", "space").with_aliasing_output_name("aliases").build()
        assert isinstance(config, OutputProjectionConfig)


class TestAliasingCompositionConfig:
    def test_when_created_with_no_args_then_uses_default_values(self) -> None:
        config = AliasingCompositionConfig()

        assert config.keys_path == JSONPath("input.keys")
        assert config.output_projection is not None
        assert config.output_projection.aliasing_output_name == "aliases"

    def test_when_created_with_custom_keys_path_then_uses_provided_path(self) -> None:
        keys_path = JSONPath("custom.path")
        config = AliasingCompositionConfig(keys_path=keys_path)

        assert config.keys_path == keys_path
        assert config.output_projection is not None

    def test_when_created_with_custom_output_projection_then_uses_provided_projection(self) -> None:
        projection = OutputProjectionConfig("custom_aliases", [FieldMapping.one_to_one("space")])
        config = AliasingCompositionConfig(output_projection=projection)

        assert config.output_projection == projection
        assert config.keys_path == JSONPath("input.keys")

    def test_when_created_with_both_custom_values_then_uses_both(self) -> None:
        keys_path = JSONPath("data.entities")
        projection = OutputProjectionConfig("custom_aliases", [FieldMapping.one_to_one("space")])
        config = AliasingCompositionConfig(keys_path=keys_path, output_projection=projection)

        assert config.keys_path == keys_path
        assert config.output_projection == projection

    def test_when_keys_path_is_none_then_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="keys_path cannot be None"):
            AliasingCompositionConfig(keys_path=None)  # type: ignore[arg-type]

    def test_when_output_projection_is_none_then_initializes_default_projection(self) -> None:
        config = AliasingCompositionConfig(output_projection=None)

        assert config.output_projection is not None
        assert config.output_projection == OutputProjectionConfig.default()

    def test_when_aliasing_composition_config_is_frozen_then_cannot_be_modified(self) -> None:
        config = AliasingCompositionConfig()
        with pytest.raises(AttributeError):
            config.keys_path = JSONPath("new.path")  # type: ignore[misc]


class TestAliasingCompositionConfigBuilder:
    def test_when_building_with_keys_path_then_config_created(self) -> None:
        builder = AliasingCompositionConfigBuilder()
        config = builder.with_keys_path("data.entities").build()

        assert config.keys_path == JSONPath("data.entities")
        assert config.output_projection is not None

    def test_when_building_with_output_projection_and_keys_path_then_config_created(self) -> None:
        projection = OutputProjectionConfig("aliases", [FieldMapping.one_to_one("space")])
        builder = AliasingCompositionConfigBuilder()
        config = builder.with_keys_path("input.data").with_output_projection(projection).build()

        assert config.output_projection == projection
        assert config.keys_path == JSONPath("input.data")

    def test_when_building_with_both_keys_path_and_projection_then_config_has_both(self) -> None:
        projection = OutputProjectionConfig("aliases", [FieldMapping.one_to_one("space")])
        builder = AliasingCompositionConfigBuilder()
        config = builder.with_keys_path("data.entities").with_output_projection(projection).build()

        assert config.keys_path == JSONPath("data.entities")
        assert config.output_projection == projection

    def test_when_with_keys_path_returns_builder_then_fluent_chaining_works(self) -> None:
        builder = AliasingCompositionConfigBuilder()
        result = builder.with_keys_path("path")
        assert isinstance(result, AliasingCompositionConfigBuilder)
        assert result is builder

    def test_when_with_output_projection_returns_builder_then_fluent_chaining_works(self) -> None:
        projection = OutputProjectionConfig("aliases", [FieldMapping.one_to_one("space")])
        builder = AliasingCompositionConfigBuilder()
        result = builder.with_output_projection(projection)
        assert isinstance(result, AliasingCompositionConfigBuilder)
        assert result is builder

    def test_when_building_without_setting_keys_path_then_raises_error(self) -> None:
        builder = AliasingCompositionConfigBuilder()
        with pytest.raises(ValueError, match="keys_path cannot be None"):
            builder.build()

    def test_when_building_without_setting_projection_then_initializes_default(self) -> None:
        builder = AliasingCompositionConfigBuilder()
        config = builder.with_keys_path("input.keys").build()

        assert config.output_projection is not None
        assert config.output_projection == OutputProjectionConfig.default()

    def test_when_builder_used_with_factory_method_then_works_correctly(self) -> None:
        builder = AliasingCompositionConfig.builder()
        config = builder.with_keys_path("input.entities").build()

        assert config.keys_path == JSONPath("input.entities")

    def test_when_chained_builder_calls_then_final_config_has_all_values(self) -> None:
        projection = OutputProjectionConfig(
            "custom_aliases",
            [
                FieldMapping("id", "external_id"),
                FieldMapping("space_name", "space"),
            ],
        )
        builder = AliasingCompositionConfig.builder()
        config = builder.with_keys_path("custom.keys").with_output_projection(projection).build()

        assert config.keys_path == JSONPath("custom.keys")
        assert config.output_projection is not None
        assert config.output_projection.aliasing_output_name == "custom_aliases"
        assert len(config.output_projection.fields) == 2

    def test_when_overriding_keys_path_then_latest_value_used(self) -> None:
        builder = AliasingCompositionConfigBuilder()
        config = builder.with_keys_path("first.path").with_keys_path("second.path").build()

        assert config.keys_path == JSONPath("second.path")

    def test_when_overriding_output_projection_then_latest_value_used(self) -> None:
        projection1 = OutputProjectionConfig("aliases1", [FieldMapping.one_to_one("space")])
        projection2 = OutputProjectionConfig("aliases2", [FieldMapping.one_to_one("external_id")])
        builder = AliasingCompositionConfigBuilder()
        config = (
            builder.with_keys_path("input.keys")
            .with_output_projection(projection1)
            .with_output_projection(projection2)
            .build()
        )

        assert config.output_projection is not None
        assert config.output_projection.aliasing_output_name == "aliases2"
