import re
from datetime import datetime
from pathlib import Path
from typing import Generic, Literal, TypeAlias, get_args

from pydantic import BaseModel, ConfigDict, DirectoryPath, Field, JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND, ResourceTypes
from cognite_toolkit._cdf_tk.cruds._base_cruds import ReadExtra, ResourceCRUD
from cognite_toolkit._cdf_tk.yaml_classes.base import T_Resource, ToolkitResource

from ._insights import ModelSyntaxWarning
from ._types import AbsoluteFilePath, RelativeDirPath, RelativeFilePath

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

    def get_pattern_replace_pair(self, file_suffix: FileSuffix = ".yaml") -> tuple[str, str]:
        substitution = self.value
        pattern = rf"{{{{\s*{self.name}\s*}}}}"
        if file_suffix in (".yaml", ".yml", ".json"):
            # Preserve data types for YAML
            if isinstance(substitution, str) and (substitution.isdigit() or substitution.endswith(":")):
                substitution = f'"{substitution}"'
                pattern = rf"'{pattern}'|{pattern}|\"{pattern}\""
            elif substitution is None:
                substitution = "null"
        elif file_suffix == ".sql":
            if isinstance(substitution, list):
                substitution = self._format_list_as_sql_tuple(substitution)
        else:
            raise NotImplementedError(f"{file_suffix!r} is not supported for variable replacement")
        return pattern, str(substitution)

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
    def substitute(cls, content: str, variables: "list[BuildVariable]", file_suffix: FileSuffix = ".yaml") -> str:
        """Substitutes variables in the given content based on their patterns and replacement values."""
        for variable in variables:
            pattern, replace = variable.get_pattern_replace_pair(file_suffix)
            content = re.sub(pattern, replace, content)
        return content


class InvalidBuildVariable(BuildVariable):
    error: ModelSyntaxWarning


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


class ModuleSource(BaseModel):
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


class BuildSource(BaseModel):
    """Class used to describe source for build"""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    module_dir: DirectoryPath = Field(description="Path to the module directory. Can be relative or absolute.")
    modules: list[ModuleSource]

    ambiguous_selection: list[AmbiguousSelection] = Field(default_factory=list)
    misplaced_modules: list[MisplacedModule] = Field(default_factory=list)
    non_existing_module_names: list[NonExistingModuleName] = Field(default_factory=list)
    invalid_variables: list[InvalidBuildVariable] = Field(default_factory=list)
    orphan_yaml_files: list[RelativeFilePath] = Field(default_factory=list)

    @property
    def total_files(self) -> int:
        return sum(module.total_files for module in self.modules)


class ResourceType(BaseModel):
    model_config = ConfigDict(frozen=True)

    resource_folder: str
    kind: str

    @property
    def crud_cls(self) -> type[ResourceCRUD]:
        kind = self.kind
        folder_name = self.resource_folder
        return RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND[folder_name][kind]


class ReadYAMLFile(BaseModel):
    source_path: AbsoluteFilePath


class FailedReadYAMLFile(ReadYAMLFile):
    code: str
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
    syntax_warning: ModelSyntaxWarning | None = None


class IgnoredFile(BaseModel):
    filepath: Path
    code: str
    reason: str


class Module(BaseModel):
    """Class used to store module in-memory"""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: ModuleId
    files: list[ReadYAMLFile] = Field(default_factory=list)
    ignored_files: list[IgnoredFile] = Field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return all(isinstance(resource, SuccessfulReadYAMLFile) for resource in self.files)
