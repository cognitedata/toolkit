from pydantic import BaseModel, ConfigDict, DirectoryPath, Field

from cognite_toolkit._cdf_tk.cruds import ResourceTypes
from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource

from ._insights import ModelSyntaxError, Recommendation
from ._types import AbsoluteFilePath, RelativeDirPath


class BuildVariable(BaseModel):
    id: RelativeDirPath
    value: str | bool | int | float | list[str | bool | int | float]
    is_selected: bool
    iteration: int | None = None

    @property
    def name(self) -> str:
        return self.id.name


class ModuleSource(BaseModel):
    """Class used to describe source for module"""

    path: DirectoryPath = Field(description="Path to the module directory. Can be relative or absolute.")
    id: RelativeDirPath = Field(description="Relative path to the organization directory.")
    resource_files_by_folder: dict[ResourceTypes, list[AbsoluteFilePath]] = Field(default_factory=dict)
    variables: list[BuildVariable] = Field(default_factory=list)
    iteration: int = 0

    @property
    def name(self) -> str:
        return self.path.name


class ResourceType(BaseModel):
    model_config = ConfigDict(frozen=True)

    resource_folder: str
    kind: str


class ReadResource(BaseModel):
    source_path: AbsoluteFilePath


class FailedReadResource(ReadResource):
    errors: list[ModelSyntaxError] = Field(default_factory=list)


class SuccessfulReadResource(ReadResource):
    source_hash: str
    resource_type: ResourceType
    resource: ToolkitResource
    insights: list[Recommendation] = Field(default_factory=list)


class Module(BaseModel):
    """Class used to store module in-memory"""

    source: ModuleSource
    resources: list[ReadResource] = Field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return all(isinstance(resource, SuccessfulReadResource) for resource in self.resources)
