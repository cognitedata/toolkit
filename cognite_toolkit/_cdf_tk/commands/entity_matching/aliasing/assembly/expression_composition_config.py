from dataclasses import dataclass, field

from cognite_toolkit._cdf_tk.commands.entity_matching.common.json_path import JSONPath


@dataclass(frozen=True)
class FieldMapping:
    output_field_name: str
    field_path: str

    def __post_init__(self) -> None:
        if not self.output_field_name:
            raise ValueError("output_field_name cannot be empty")
        if not self.field_path:
            raise ValueError("field_path cannot be empty")

    @staticmethod
    def one_to_one(output_name: str) -> "FieldMapping":
        return FieldMapping(output_name, output_name)


@dataclass(frozen=True)
class OutputProjectionConfig:
    aliasing_output_name: str
    fields: list[FieldMapping] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.aliasing_output_name:
            raise ValueError("aliasing_output_name cannot be empty")
        if not self.fields:
            raise ValueError("fields list cannot be empty")

    def generate_projection_object(self, entity_name: str, aliasing_expr: str) -> str:
        field_parts = [f'"{field.output_field_name}": {entity_name}.{field.field_path}' for field in self.fields]
        field_parts.append(f'"{self.aliasing_output_name}": {aliasing_expr}')
        return "{" + ", ".join(field_parts) + "}"

    @staticmethod
    def builder() -> "OutputProjectionConfigBuilder":
        return OutputProjectionConfigBuilder()

    @staticmethod
    def default() -> "OutputProjectionConfig":
        return OutputProjectionConfig(
            aliasing_output_name="aliases",
            fields=[
                FieldMapping.one_to_one("space"),
                FieldMapping.one_to_one("external_id"),
                FieldMapping.one_to_one("keys"),
            ],
        )


class OutputProjectionConfigBuilder:
    def __init__(self) -> None:
        self._fields: list[FieldMapping] = []
        self._aliasing_output_name: str | None = None

    def with_field_from_entity(self, output_name: str, field_path: str) -> "OutputProjectionConfigBuilder":
        if not output_name:
            raise ValueError("output_name cannot be empty")
        if not field_path:
            raise ValueError("field_path cannot be empty")

        self._fields.append(FieldMapping(output_name, field_path))
        return self

    def with_aliasing_output_name(self, name: str) -> "OutputProjectionConfigBuilder":
        if not name:
            raise ValueError("aliasing_output_name cannot be empty")
        self._aliasing_output_name = name
        return self

    def build(self) -> OutputProjectionConfig:
        if not self._fields:
            raise ValueError("At least one field must be added before building")
        if not self._aliasing_output_name:
            raise ValueError("aliasing_output_name must be set before building")

        return OutputProjectionConfig(
            aliasing_output_name=self._aliasing_output_name,
            fields=self._fields,
        )


@dataclass(frozen=True)
class AliasingCompositionConfig:
    keys_path: JSONPath = field(default_factory=lambda: JSONPath("input.keys"))
    output_projection: OutputProjectionConfig | None = None

    def __post_init__(self) -> None:
        if not self.keys_path:
            raise ValueError("keys_path cannot be None")
        if self.output_projection is None:
            object.__setattr__(self, "output_projection", OutputProjectionConfig.default())

    @staticmethod
    def builder() -> "AliasingCompositionConfigBuilder":
        return AliasingCompositionConfigBuilder()


class AliasingCompositionConfigBuilder:
    def __init__(self) -> None:
        self._keys_path: JSONPath | None = None
        self._output_projection: OutputProjectionConfig | None = None

    def with_keys_path(self, path: str) -> "AliasingCompositionConfigBuilder":
        self._keys_path = JSONPath(path)
        return self

    def with_output_projection(self, projection: OutputProjectionConfig) -> "AliasingCompositionConfigBuilder":
        self._output_projection = projection
        return self

    def build(self) -> AliasingCompositionConfig:
        if self._keys_path is None:
            raise ValueError("keys_path cannot be None")
        return AliasingCompositionConfig(keys_path=self._keys_path, output_projection=self._output_projection)
