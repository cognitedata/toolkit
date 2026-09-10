import json
import re
import sys
import uuid
from collections.abc import Callable
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Any, ClassVar, Generic, Literal, TypeAlias, get_args

from pydantic import BaseModel, ConfigDict, DirectoryPath, Field, JsonValue, field_validator
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.resource_ios import RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND, ResourceTypes
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ReadExtra, ResourceIO
from cognite_toolkit._cdf_tk.yaml_classes.base import T_Resource, ToolkitResource

from ._insights import ModelSyntaxError, ModelSyntaxWarning
from ._types import AbsoluteFilePath, RelativeDirPath, RelativeFilePath

if sys.version_info >= (3, 11):
    from typing import Self

    import tomllib as toml
else:
    import tomli as toml
    from typing_extensions import Self


FileSuffix: TypeAlias = Literal[".yaml", ".sql", ".yml", ".json"]
SUPPORTS_VARIABLE_REPLACEMENT = frozenset(get_args(FileSuffix))


class BuildVariable(BaseModel):
    id: RelativeDirPath
    value: str | bool | int | float | list[str | bool | int | float]
    is_selected: bool
    iteration: int | None = None

    @property
    def name(self) -> str:
        return self.id.name

    def get_pattern_replace_pair(
        self, file_suffix: FileSuffix | str = ".yaml"
    ) -> tuple[str, str | Callable[[re.Match[str]], str]]:
        substitution = self.value
        pattern = rf"{{{{\s*{self.name}\s*}}}}"
        if file_suffix in (".yaml", ".yml", ".json"):
            # Preserve data types for YAML
            if isinstance(substitution, str) and (substitution.isdigit() or substitution.endswith(":")):
                substitution = f'"{substitution}"'
                pattern = rf"'{pattern}'|{pattern}|\"{pattern}\""
            elif substitution is None:
                substitution = "null"
            elif isinstance(substitution, list) and (file_suffix == ".yaml" or file_suffix == ".yml"):
                variable_token = rf"{{{{\s*{re.escape(self.name)}\s*}}}}"
                pattern = rf"(?m)^(?P<indent>[ \t]*){variable_token}\s*$|{variable_token}"
                values = substitution

                def replace_yaml_list(match: re.Match[str]) -> str:
                    if (indent := match.group("indent")) is not None:
                        return "\n".join(f"{indent}- {self._yaml_block_sequence_scalar(item)}" for item in values)
                    return str(substitution)

                return pattern, replace_yaml_list
        elif file_suffix == ".sql":
            if isinstance(substitution, list):
                substitution = self._format_list_as_sql_tuple(substitution)
        else:
            raise NotImplementedError(f"{file_suffix!r} is not supported for variable replacement")
        return pattern, str(substitution)

    @staticmethod
    def _yaml_block_sequence_scalar(value: str | bool | int | float) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int | float):
            return str(value)
        if (
            value.isdigit()
            or value.endswith(":")
            or value.lower() in ("true", "false", "null", "yes", "no", "on", "off")
        ):
            return json.dumps(value)
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*", value):
            return value
        return json.dumps(value)

    @staticmethod
    def _format_list_as_sql_tuple(replace: list[str | bool | int | float]) -> str:
        """Format a list as a SQL-style tuple string.

        Args:
            replace: The list to format

        Returns:
            SQL tuple string, e.g., "('A', 'B', 'C')" or "()" for empty lists
        """
        if not replace:
            # Empty list becomes empty SQL tuple
            return "()"
        else:
            # Format list as SQL tuple: ('A', 'B', 'C')
            formatted_items = []
            for item in replace:
                if item is None:
                    formatted_items.append("NULL")
                elif isinstance(item, str):
                    formatted_items.append(f"'{item}'")
                else:
                    formatted_items.append(str(item))
            return f"({', '.join(formatted_items)})"

    @classmethod
    def substitute(cls, content: str, variables: "list[BuildVariable]", file_suffix: FileSuffix | str = ".yaml") -> str:
        """Substitutes variables in the given content based on their patterns and replacement values."""
        for variable in variables:
            pattern, replace = variable.get_pattern_replace_pair(file_suffix)
            content = re.sub(pattern, (lambda _: replace) if isinstance(replace, str) else replace, content)
        return content

    @classmethod
    def substitute_with_placeholders(
        cls, content: str, variables: "list[BuildVariable]"
    ) -> tuple[str, dict[str, "BuildVariable"]]:
        """Replace ``{{ variable }}`` tokens with unique placeholders.

        Used by pull to round-trip CDF values back into source files without
        resolving template variables. The placeholders are valid YAML tokens so
        the content can be parsed, then swapped back to ``{{ name }}`` syntax.
        """
        variable_by_placeholder: dict[str, BuildVariable] = {}
        for variable in variables:
            placeholder = f"VARIABLE_{uuid.uuid4().hex[:8]}"
            variable_by_placeholder[placeholder] = variable
            pattern = rf"{{{{\s*{re.escape(variable.name)}\s*}}}}"
            content = re.sub(pattern, placeholder, content)
        return content, variable_by_placeholder


class InvalidBuildVariable(BuildVariable):
    error: ModelSyntaxError


class ModuleId(Identifier):
    model_config = ConfigDict(frozen=True)
    id: RelativeDirPath
    path: DirectoryPath

    def __str__(self) -> str:
        return str(self.id)

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"id-{self.id}"
        return str(self.id)

    @property
    def name(self) -> str:
        return self.id.name


class ExampleData(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    repo_type: str
    repo: str
    source: str
    destination: Path


class ModuleToml(BaseModel):
    filename: ClassVar[str] = "module.toml"
    title: str | None
    id: str | None = None
    dependencies: frozenset[str] = Field(default_factory=frozenset)
    is_selected_by_default: bool = False
    data: list[ExampleData] = Field(default_factory=list)
    extra_resources: list[Path] = Field(default_factory=list)
    package_id: str | None = None

    @field_validator("extra_resources")
    @classmethod
    def validate_extra_resources(cls, v: list[Path]) -> list[Path]:
        for extra in v:
            if extra.is_absolute():
                raise ValueError(f"Extra resource {extra} must be a relative path")
        return v

    @classmethod
    def load(cls, data: dict[str, Any] | Path) -> Self:
        if isinstance(data, Path):
            return cls.load(toml.loads(data.read_text(encoding="utf-8")))

        if "dependencies" in data:
            dependencies = frozenset(data["dependencies"].get("modules", set()))
        else:
            dependencies = frozenset()

        example_data: list[ExampleData] = []
        if "data" in data and isinstance(data["data"], list):
            example_data = [ExampleData.model_validate(d) for d in data["data"]]

        extra_resources: list[Path] = []
        if "extra_resources" in data and isinstance(data["extra_resources"], list):
            extra_resources = [Path(item["location"]) for item in data["extra_resources"] if "location" in item]

        title: str | None = None
        id: str | None = None
        is_selected_by_default: bool = False
        package_id: str | None = None
        if "module" in data:
            title = data["module"].get("title")
            id = data["module"].get("id")
            is_selected_by_default = data["module"].get("is_selected_by_default", False)
            package_id = data["module"].get("package_id")

        return cls(
            title=title,
            id=id,
            dependencies=dependencies,
            is_selected_by_default=is_selected_by_default,
            data=example_data,
            extra_resources=extra_resources,
            package_id=package_id,
        )


class ModuleDirectory(BaseModel):
    """Class used to describe source for module"""

    id: RelativeDirPath = Field(description="Relative path to the organization directory.")
    path: DirectoryPath = Field(description="Path to the module directory. Can be relative or absolute.")
    resource_files_by_folder: dict[ResourceTypes, list[AbsoluteFilePath]] = Field(default_factory=dict)
    variables: list[BuildVariable] = Field(default_factory=list)
    iteration: int = 0

    @property
    def name(self) -> str:
        return self.path.name

    def as_id(self) -> ModuleId:
        return ModuleId(id=self.id, path=self.path)

    @property
    def total_files(self) -> int:
        return sum(len(files) for files in self.resource_files_by_folder.values())

    @cached_property
    def module_toml(self) -> ModuleToml | None:
        module_toml_path = self.path / ModuleToml.filename
        if module_toml_path.exists():
            return ModuleToml.load(module_toml_path)
        return None

    @property
    def has_example_data(self) -> bool:
        return bool(self.module_toml and self.module_toml.data)

    @property
    def title(self) -> str | None:
        """The title of the module."""
        if self.module_toml:
            return self.module_toml.title
        return None

    @property
    def module_id(self) -> str | None:
        """The ID of the module."""
        if self.module_toml:
            return self.module_toml.id
        return None

    @property
    def package_id(self) -> str | None:
        """The ID of the package."""
        if self.module_toml:
            return self.module_toml.package_id
        return None


class AmbiguousSelection(BaseModel):
    name: str
    module_paths: list[RelativeDirPath]
    is_selected: bool


class MisplacedModule(BaseModel):
    id: RelativeDirPath
    parent_modules: list[RelativeDirPath]


class NonExistingModuleName(BaseModel):
    name: str
    closest_matches: list[str]


class ModuleScanResult(BaseModel):
    """Class used to describe source for build"""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    module_dir: DirectoryPath = Field(description="Path to the module directory. Can be relative or absolute.")
    modules: list[ModuleDirectory]

    ambiguous_selection: list[AmbiguousSelection] = Field(default_factory=list)
    misplaced_modules: list[MisplacedModule] = Field(default_factory=list)
    non_existing_module_names: list[NonExistingModuleName] = Field(default_factory=list)
    invalid_variables: list[InvalidBuildVariable] = Field(default_factory=list)
    orphan_yaml_files: list[RelativeFilePath] = Field(default_factory=list)

    @property
    def total_files(self) -> int:
        return sum(module.total_files for module in self.modules)

    @property
    def all_variables(self) -> list[BuildVariable]:
        variables: dict[Path, BuildVariable] = {}
        for module in self.modules:
            variables.update({variable.id: variable for variable in module.variables})
        return list(variables.values())


class ResourceType(BaseModel):
    model_config = ConfigDict(frozen=True)

    resource_folder: str
    kind: str

    @property
    def crud_cls(self) -> type[ResourceIO]:
        kind = self.kind
        folder_name = self.resource_folder
        return RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND[folder_name][kind]

    def load_identifier(self, data: dict[str, Any]) -> Identifier:
        return self.crud_cls.get_id(data)

    def __str__(self) -> str:
        return f"{self.kind} ({self.resource_folder})"


class ReadYAMLFile(BaseModel):
    source_path: AbsoluteFilePath
    unresolved_variables: list[str] = Field(default_factory=list)


class FailedReadYAMLFile(ReadYAMLFile):
    code: Literal["MISSING-SUFFIX", "INVALID-KIND", "READ-ERROR", "YAML-PARSE-ERROR", "EMPTY-FILE"]
    error: str


class ReadResource(BaseModel, Generic[T_Resource]):
    raw: dict[str, JsonValue | datetime]
    identifier: Identifier
    validated: ToolkitResource | None = None
    extra_files: list[ReadExtra] = Field(default_factory=list)


class SuccessfulReadYAMLFile(ReadYAMLFile):
    source_hash: str
    resource_type: ResourceType
    resources: list[ReadResource[ToolkitResource]]
    syntax_error: ModelSyntaxError | None = None
    syntax_warning: ModelSyntaxWarning | None = None
    line_count: int


class IgnoredFile(BaseModel):
    filepath: Path
    code: Literal["MISSING-SUFFIX"]
    reason: str
    fix: str


class Module(BaseModel):
    """Class used to store module in-memory"""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: ModuleId
    files: list[ReadYAMLFile] = Field(default_factory=list)
    ignored_files: list[IgnoredFile] = Field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return all(isinstance(resource, SuccessfulReadYAMLFile) for resource in self.files)
