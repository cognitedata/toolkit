from collections import defaultdict
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from ._insights import InsightList
from ._module import ModuleSource


class BuildParameters(BaseModel):
    organization_dir: Path
    build_dir: Path = Field(default_factory=lambda: Path.cwd() / "build")
    config_yaml_name: str | None = Field(
        None,
        description="The name of the configuration YAML file to use. It expected to be"
        "named config.[name].yaml and be located in the organization directory.",
    )
    user_selected_modules: list[str] | None = Field(
        None,
        description="List of module names or paths to build. If not provided, Toolkit will first attempt to find a config YAML "
        "and the modules specified there. If no config YAML is found, Toolkit will build all modules in the organization directory.",
    )


class BuiltModule(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    source: ModuleSource
    built_files: list[Path] = Field(default_factory=list)

    insights: InsightList = Field(default_factory=InsightList)


class BuildLinage(BaseModel): ...


class BuildFolder(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    resource_by_type: dict[str, dict[str, list[Path]]] = Field(default_factory=dict)
    insights: InsightList = Field(default_factory=InsightList)
    linage: BuildLinage = Field(default_factory=BuildLinage)

    def add_build_files(self, files: list[Path]) -> None:
        """Adds build files to resource_by_type, organizing them by type and folder."""

        result: dict[str, dict[str, list[Path]]] = defaultdict(lambda: defaultdict(list))
        for file in files:
            resource_type = file.stem.split(".")[-1]
            resource_type_folder = file.parent.name
            result[resource_type_folder][resource_type].append(file)

        self.resource_by_type = self.resource_by_type | dict(result)
