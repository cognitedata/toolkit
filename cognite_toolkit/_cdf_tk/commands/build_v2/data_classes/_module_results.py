from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import InsightList


class ModuleResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str = Field(..., description="Name of the module, typically the name of the folder containing the module.")
    built_files: list[Path] = Field(default_factory=list)
    insights: InsightList = Field(default_factory=InsightList)
