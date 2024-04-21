# from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
# Error
# MigrationError
# VersionError
# ModuleVersionError
# EnvError
# MissingResourceError
# CleanResourceError
# DeployResourceError
# MissingModuleError
# DuplicatedResourceError
# DuplicatedModuleError
# NotADirectoryError
# IsADirectoryError
# FileNotFoundError
# FileExistsError
# ValidationError
# YAMLFormatError

from yaml import YAMLError


class ToolkitError(Exception):
    pass


class ToolkitInvalidSettingsError(Exception):
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
        dupe_info = [f"    {module_name}: {paths}" for module_name, paths in self.duplicated.items()]
        return "\n".join([super().__str__(), *dupe_info])


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
