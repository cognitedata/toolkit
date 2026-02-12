from pathlib import Path

from pydantic import ConfigDict, Field

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import InsightList

from ._module import Module


class ModuleResult(Module):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    built_files: list[Path] = Field(default_factory=list)
    insights: InsightList = Field(default_factory=InsightList)
