from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from cognite_toolkit._cdf_tk.constants import MODULES

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

    @property
    def modules_directory(self) -> Path:
        return self.organization_dir / MODULES


class BuiltModule(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    source: ModuleSource
    built_files: list[Path] = Field(default_factory=list)

    insights: InsightList = Field(default_factory=InsightList)


class BuildLineage(BaseModel): ...


class BuildFolder(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    resource_by_type: dict[str, dict[str, list[Path]]] = Field(default_factory=dict)
    insights: InsightList = Field(default_factory=InsightList)
    lineage: BuildLineage = Field(default_factory=BuildLineage)

    def add_build_files(self, files: list[Path]) -> None:
        """Adds build files to resource_by_type, organizing them by type and folder."""

        for file in files:
            resource_type = file.stem.split(".")[-1]
            resource_type_folder = file.parent.name
            self.resource_by_type.setdefault(resource_type_folder, {}).setdefault(resource_type, []).append(file)
