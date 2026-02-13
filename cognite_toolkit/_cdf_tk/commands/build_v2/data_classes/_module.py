from pydantic import BaseModel, ConfigDict, DirectoryPath, Field

from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource

from ._insights import InsightList
from ._types import AbsoluteFilePath, RelativeDirPath


class BuildVariable(BaseModel):
    id: RelativeDirPath
    value: str | bool | int | float | list[str] | list[int] | list[float] | list[bool]
    is_selected: bool
    iteration: int | None = None

    @property
    def name(self) -> str:
        return self.id.name


class ModuleSource(BaseModel):
    """Class used to describe source for module"""

    path: DirectoryPath = Field(description="Path to the module directory. Can be relative or absolute.")
    id: RelativeDirPath = Field(description="Relative path to the organization directory.")
    resource_files: list[AbsoluteFilePath] = Field(default_factory=list)
    variables: list[BuildVariable] = Field(default_factory=list)
    iteration: int = 0

    @property
    def name(self) -> str:
        return self.path.name


class ResourceType(BaseModel):
    model_config = ConfigDict(frozen=True)

    resource_folder: str
    kind: str


class Module(BaseModel):
    """Class used to store module in-memory"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    source: ModuleSource

    resources_by_type: dict[ResourceType, list[ToolkitResource]]
    insights: InsightList = Field(default_factory=InsightList)

    @property
    def is_success(self) -> bool:
        return not self.insights.has_model_syntax_errors
