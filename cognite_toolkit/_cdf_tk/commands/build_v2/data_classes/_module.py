from collections import UserList
from pathlib import Path
from typing import Annotated, TypeAlias
from pydantic import BaseModel, Field
from pydantic import ConfigDict, Field
from pydantic import DirectoryPath


from pydantic import BaseModel, PlainValidator
from pydantic import ConfigDict, Field
from pydantic import DirectoryPath

from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource
from ._insights import InsightList
from ._types import RelativeDirPath
from ._types import RelativeDirPath

class BuildVariable(BaseModel): ...

class ModuleSource(BaseModel):
    """Class used to describe source for module"""

class Module(BaseModel):
    path: RelativeDirPath
    iteration: int = 0
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
