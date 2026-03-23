from functools import cached_property

from pydantic import BaseModel, ConfigDict, DirectoryPath, Field

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND, ResourceTypes
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.yaml_classes.base import ToolkitResource

from ._insights import InsightList, ModelSyntaxError
from ._types import AbsoluteFilePath, RelativeDirPath


class BuildVariable(BaseModel):
    id: RelativeDirPath
    value: str | bool | int | float | list[str | bool | int | float]
    is_selected: bool
    iteration: int | None = None

    @property
    def name(self) -> str:
        return self.id.name


class ModuleId(Identifier):
    model_config = ConfigDict(frozen=True)
    id: RelativeDirPath
    path: DirectoryPath

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

    def as_id(self) -> ModuleId:
        return ModuleId(id=self.id, path=self.path)


class ResourceType(BaseModel):
    model_config = ConfigDict(frozen=True)

    resource_folder: str
    kind: str


class ReadResource(BaseModel):
    source_path: AbsoluteFilePath


class FailedReadResource(ReadResource):
    errors: list[ModelSyntaxError] = Field(default_factory=list)


class SuccessfulReadResource(ReadResource):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    source_hash: str
    resource_type: ResourceType
    resource: ToolkitResource
    insights: InsightList = Field(default_factory=InsightList)

    @cached_property
    def dependencies(self) -> set[tuple[type[ResourceCRUD], Identifier]]:
        kind = self.resource_type.kind
        folder_name = self.resource_type.resource_folder
        crud = RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND[folder_name][kind]
        return set(crud.get_dependencies(self.resource))


class Module(BaseModel):
    """Class used to store module in-memory"""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    # Todo: Replace module source with ModuleId
    source: ModuleSource
    resources: list[ReadResource] = Field(default_factory=list)
    # Todo: Remove insights from this location. It is only used in the orchestrator.
    insights: InsightList = Field(default_factory=InsightList)

    @property
    def is_success(self) -> bool:
        return not self.insights.has_errors and all(
            isinstance(resource, SuccessfulReadResource) for resource in self.resources
        )

    @cached_property
    def dependencies(self) -> dict[AbsoluteFilePath, set[tuple[type[ResourceCRUD], Identifier]]]:
        """Get external dependencies for all resources in the module."""
        dependencies: dict[AbsoluteFilePath, set[tuple[type[ResourceCRUD], Identifier]]] = {}

        for resource in self.resources:
            if not isinstance(resource, SuccessfulReadResource):
                continue
            dependencies[resource.source_path] = resource.dependencies
        return dependencies
