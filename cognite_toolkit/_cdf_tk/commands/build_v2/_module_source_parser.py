from typing import Any

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    AbsoluteDirPath,
    InsightList,
    ModuleSource,
    RelativeDirPath,
)


class ModuleSourceParser:
    MODULE_ERROR_CODE = "MOD_001"
    VARIABLE_ERROR_CODE = "CONFIG_VARIABLE_001"

    def __init__(
        self,
        yaml_files: list[RelativeDirPath],
        variables: dict[str, Any],
        selected_modules: set[RelativeDirPath | str],
        organization_dir: AbsoluteDirPath,
    ) -> None:
        self.yaml_files = yaml_files
        self.variables = variables
        self.selected_modules = selected_modules
        self.organization_dir = organization_dir
        self.errors = InsightList()

    def parse(self) -> list[ModuleSource]:
        raise NotImplementedError("")
