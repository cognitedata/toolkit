from collections import UserList
from pathlib import Path
from typing import Annotated, TypeAlias

from pydantic import BaseModel, PlainValidator
from pydantic import ConfigDict, Field
from pydantic import DirectoryPath

from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource
from ._insights import InsightList

RelativeDirPath: TypeAlias = Annotated[
    Path, PlainValidator(lambda p: p if p.is_dir() and p.is_relative() else ValueError(f"{p} is not a directory"))
]

class BuildVariable(BaseModel): ...

class ModuleSource(BaseModel):
    """Class used to describe source for module"""

    path: DirectoryPath
    iteration: int = 0
    variables: list[BuildVariable]

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
