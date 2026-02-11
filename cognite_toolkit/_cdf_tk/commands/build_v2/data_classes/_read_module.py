from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import InsightList
from cognite_toolkit._cdf_tk.resource_classes.base import ToolkitResource


class ResourceType(BaseModel):
    model_config = ConfigDict(frozen=True)

    resource_folder: str
    kind: str


class ReadModule(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(..., description="Name of the module, typically the name of the folder containing the module.")
    resources_by_type: dict[ResourceType, list[ToolkitResource]]
    insights: InsightList = Field(default_factory=InsightList)

    @property
    def is_success(self) -> bool:
        return not self.insights.has_model_syntax_errors
