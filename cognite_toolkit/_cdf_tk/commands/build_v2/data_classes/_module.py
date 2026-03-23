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

    @property
    def total_files(self) -> int:
        return sum(len(files) for files in self.resource_files_by_folder.values())


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

    @property
    def crud_cls(self) -> type[ResourceCRUD]:
        kind = self.resource_type.kind
        folder_name = self.resource_type.resource_folder
        return RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND[folder_name][kind]

    @property
    def dependencies(self) -> set[tuple[type[ResourceCRUD], Identifier]]:
        return set(self.crud_cls.get_dependencies(self.resource))


class Module(BaseModel):
    """Class used to store module in-memory"""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    id: ModuleId
    resources: list[ReadResource] = Field(default_factory=list)

    @property
    def is_success(self) -> bool:
        return all(isinstance(resource, SuccessfulReadResource) for resource in self.resources)
