from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, JsonValue

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import RelativeDirPath
from cognite_toolkit._cdf_tk.constants import MODULES


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


class ParseInput(BaseModel):
    yaml_files: list[Path]
    selected_modules: set[RelativeDirPath | str]
    variables: dict[str, JsonValue]
    validation_type: Literal["dev", "prod"] = "prod"
    cdf_project: str
