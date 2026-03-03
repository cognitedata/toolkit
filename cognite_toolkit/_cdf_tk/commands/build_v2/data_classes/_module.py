from functools import cached_property

from pydantic import BaseModel, ConfigDict, DirectoryPath, Field

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND, ResourceTypes
from cognite_toolkit._cdf_tk.yaml_classes.base import ToolkitResource

from ._insights import ConsistencyError, ModelSyntaxError, Recommendation
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
    insights: list[Recommendation | ConsistencyError] = Field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return all(isinstance(resource, SuccessfulReadResource) for resource in self.resources)

    @cached_property
    def dependencies(self) -> dict[AbsoluteFilePath, dict[type[ToolkitResource], set[Identifier]]]:
        """Get external dependencies for all resources in the module."""
        dependencies: dict[AbsoluteFilePath, dict[type[ToolkitResource], set[Identifier]]] = {}

        for resource in self.resources:
            if not isinstance(resource, SuccessfulReadResource):
                continue

            # get crud for the given resource to be able to get dependencies
            kind = resource.resource_type.kind
            folder_name = resource.resource_type.resource_folder
            crud = RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND[folder_name][kind]

            dependencies[resource.source_path] = crud.get_dependencies(resource.resource)

        return dependencies
