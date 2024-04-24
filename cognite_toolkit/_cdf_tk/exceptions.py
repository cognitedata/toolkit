from yaml import YAMLError

from ._constants import MODULE_PATH_SEP


class ToolkitError(Exception):
    pass


class ToolkitInvalidSettingsError(ToolkitError):
    pass


class ToolkitMigrationError(ToolkitError):
    pass


class ToolkitVersionError(ToolkitError):
    pass


class ToolkitModuleVersionError(ToolkitError):
    pass


class ToolkitEnvError(ToolkitError):
    pass


class ToolkitMissingResourceError(ToolkitError):
    pass


class ToolkitCleanResourceError(ToolkitError):
    pass


class ToolkitDeployResourceError(ToolkitError):
    pass


class ToolkitMissingModuleError(ToolkitError):
    pass


class ToolkitDuplicatedResourceError(ToolkitError):
    pass


class ToolkitDuplicatedModuleError(ToolkitError):
    def __init__(self, message: str, duplicated: dict[str, list]) -> None:
        super().__init__(message)
        self.duplicated = duplicated

    def __str__(self) -> str:
        lines = [super().__str__()]
        for module_name, paths in self.duplicated.items():
            locations = "\n        ".join(sorted(MODULE_PATH_SEP.join(path) for path in paths))
            lines.append(f"    {module_name} exists in:\n        {locations}")
        lines.append(
            "    You can use the path syntax to disambiguate between modules with the same name. For example "
            "'cognite_modules/core/cdf_apm_base' instead of 'cdf_apm_base'."
        )
        return "\n".join(lines)


class ToolkitNotADirectoryError(NotADirectoryError, ToolkitError):
    pass


class ToolkitIsADirectoryError(IsADirectoryError, ToolkitError):
    pass


class ToolkitFileNotFoundError(FileNotFoundError, ToolkitError):
    pass


class ToolkitFileExistsError(FileExistsError, ToolkitError):
    pass


class ToolkitValidationError(ToolkitError):
    pass


class ToolkitYAMLFormatError(YAMLError, ToolkitValidationError):
    pass
