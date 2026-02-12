from collections import UserList

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource

from ._insights import InsightList
from ._types import RelativeDirPath


class BuildVariable(BaseModel): ...


class ModuleSource(BaseModel):
    """Class used to describe source for module"""

    path: RelativeDirPath
    iteration: int = 0
    resource_files: list[RelativeDirPath] = Field(default_factory=list)
    variables: list[BuildVariable] = Field(default_factory=list)

    @property
    def name(self) -> str:
        return self.path.name


class ModuleSources(UserList[ModuleSource]): ...


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
