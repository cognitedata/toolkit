from functools import cached_property
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

    @property
    def resource_by_type(self) -> dict[str, dict[str, list[Path]]]:
        """Organizes built files by their resource type."""
        resource_by_type: dict[str, dict[str, list[Path]]] = {}
        for file in self.built_files:
            resource_type = file.stem.split(".")[-1]
            resource_type_folder = file.parent.name
            resource_by_type.setdefault(resource_type_folder, {}).setdefault(resource_type, []).append(file)

        return resource_by_type

    @property
    def is_success(self) -> bool:
        return True if self.built_files else False


class BuildLineage(BaseModel): ...


class BuildFolder(BaseModel):
    """Built folder acts as a container holding all built modules and insights from the build process."""

    model_config = ConfigDict(arbitrary_types_allowed=True)
    path: Path
    built_modules: list[BuiltModule] = Field(default_factory=list)

    @cached_property
    def insights(self) -> InsightList:
        """Insights from all built modules."""
        insights = InsightList()
        for module in self.built_modules:
            insights.extend(module.insights)
        return insights

    @property
    def linage(self) -> BuildLineage:
        """Linage should be generated based on the built modules, but for now it is just a placeholder."""
        return BuildLineage()

    @property
    def built_modules_by_success(self) -> dict[bool, list[str]]:
        """Organizes built modules by their success status."""
        modules_by_success: dict[bool, list[str]] = {True: [], False: []}
        for built_module in self.built_modules:
            modules_by_success[built_module.is_success].append(built_module.source.name)

        return modules_by_success
