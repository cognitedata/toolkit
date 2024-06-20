from __future__ import annotations

from collections.abc import Mapping

from yaml import YAMLError


class ToolkitError(Exception):
    def __repr__(self) -> str:
        # Repr is what is called by rich when the exception is printed.
        return str(self)


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


class ToolkitMissingModulesError(ToolkitError):
    pass


class ToolkitDuplicatedResourceError(ToolkitError):
    pass


class ToolkitDuplicatedModuleError(ToolkitError):
    def __init__(self, message: str, duplicated: dict[str, list]) -> None:
        super().__init__(message)
        self.duplicated = duplicated

    def __str__(self) -> str:
        from cognite_toolkit._cdf_tk.constants import MODULE_PATH_SEP

        lines = [super().__str__()]
        for module_name, paths in self.duplicated.items():
            locations = "\n        ".join(sorted(MODULE_PATH_SEP.join(path) for path in paths))
            lines.append(f"    {module_name} exists in:\n        {locations}")
        lines.append(
            "    You can use the path syntax to disambiguate between modules with the same name. For example "
            "'cognite_modules/core/cdf_apm_base' instead of 'cdf_apm_base'."
        )
        return "\n".join(lines)

    def __repr__(self) -> str:
        # Repr is what is called by rich when the exception is printed.
        return str(self)


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


class ToolkitInvalidParameterNameError(ToolkitValidationError):
    def __init__(self, message: str, identifier: str, correct_by_wrong_parameter: Mapping[str, str | None]) -> None:
        super().__init__(message)
        self.identifier = identifier
        self.parameter = correct_by_wrong_parameter

    def __str__(self) -> str:
        parameters = []
        for wrong, correct in self.parameter.items():
            if correct is not None:
                parameters.append(f"{wrong!r} should be {correct!r}")
            else:
                parameters.append(f"{wrong!r} is invalid")
        parameter_str = "    \n".join(parameters)
        message = super().__str__()
        return f"{message}\nIn {self.identifier!r} the following parameters are invalid: {parameter_str}"

    def __repr__(self) -> str:
        # Repr is what is called by rich when the exception is printed.
        return str(self)


class ToolkitValueError(ValueError, ToolkitError):
    pass


class ToolkitRequiredValueError(ToolkitError, ValueError):
    pass


class ToolkitResourceMissingError(ToolkitError):
    def __init__(self, message: str, resource: str) -> None:
        super().__init__(message)
        self.resource = resource

    def __str__(self) -> str:
        return f"{super().__str__()}\nResource {self.resource!r} is missing"


class UploadFileError(ToolkitError):
    pass


class ResourceRetrievalError(ToolkitError): ...


class ResourceCreationError(ToolkitError):
    pass


class ResourceDeleteError(ToolkitError): ...


class ResourceUpdateError(ToolkitError):
    pass


class AmbiguousResourceFileError(ToolkitError):
    pass


class AuthenticationError(ToolkitError):
    pass


class AuthorizationError(ToolkitError):
    pass
