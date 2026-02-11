from pathlib import Path

from pydantic import ConfigDict, Field

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import InsightList

from ._module import Module


class ModuleResult(Module):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    built_files: list[Path] = Field(default_factory=list)
    insights: InsightList = Field(default_factory=InsightList)

    @property
    def built_files_per_resource_type(self) -> dict[str, list[Path]]:
        """Returns a dictionary of built files sorted by their resource type."""
        result: dict[str, list[Path]] = {}
        for file in self.built_files:
            resource_type = file.parent.name
            if resource_type not in result:
                result[resource_type] = []
            result[resource_type].append(file)
        return result
