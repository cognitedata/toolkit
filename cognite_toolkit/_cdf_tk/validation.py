import inspect
import re
from pathlib import Path
from typing import Any, TypeVar

from cognite.client.data_classes._base import CogniteObject
from cognite.client.utils._text import to_snake_case
from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic_core import ErrorDetails

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import DEV_ONLY_MODULES
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, BuildVariables, ModuleDirectories
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitDuplicatedModuleError,
    ToolkitEnvError,
    ToolkitMissingModuleError,
)
from cognite_toolkit._cdf_tk.hints import ModuleDefinition
from cognite_toolkit._cdf_tk.resource_classes import BaseModelResource
from cognite_toolkit._cdf_tk.tk_warnings import (
    DataSetMissingWarning,
    MediumSeverityWarning,
    TemplateVariableWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection

__all__ = [
    "humanize_validation_error",
    "validate_data_set_is_set",
    "validate_module_selection",
    "validate_modules_variables",
]


T_BaseModel = TypeVar("T_BaseModel", bound=BaseModel)


def validate_modules_variables(variables: BuildVariables, filepath: Path) -> WarningList:
    """Checks whether the config file has any issues.

    Currently, this checks for:
        * Non-replaced template variables, such as <change_me>.

    Args:
        variables: The variables to check.
        filepath: The filepath of the config.yaml.
    """
    warning_list: WarningList = WarningList()
    pattern = re.compile(r"<.*?>")
    for variable in variables:
        if isinstance(variable.value, str) and pattern.match(variable.value):
            warning_list.append(
                TemplateVariableWarning(filepath, variable.value, variable.key, ".".join(variable.location.parts))
            )
    return warning_list


def validate_data_set_is_set(
    raw: dict[str, Any] | list[dict[str, Any]],
    resource_cls: type[CogniteObject],
    filepath: Path,
    identifier_key: str = "externalId",
) -> WarningList:
    warning_list: WarningList = WarningList()
    signature = inspect.signature(resource_cls.__init__)
    if "data_set_id" not in set(signature.parameters.keys()):
        return warning_list

    if isinstance(raw, list):
        for item in raw:
            warning_list.extend(validate_data_set_is_set(item, resource_cls, filepath, identifier_key))
        return warning_list

    if "dataSetExternalId" in raw or "dataSetId" in raw:
        return warning_list

    value = raw.get(identifier_key, raw.get(to_snake_case(identifier_key), f"No identifier {identifier_key}"))
    warning_list.append(DataSetMissingWarning(filepath, value, identifier_key, resource_cls.__name__))
    return warning_list


def validate_resource_yaml_pydantic(
    data: dict[str, object] | list[dict[str, object]], validation_cls: type[BaseModelResource], source_file: Path
) -> WarningList:
    """Validates the resource given as a dictionary or list of dictionaries with the given pydantic model.

    Args:
        data: The data to validate.
        validation_cls: The pydantic model to validate against.
        source_file: The source file of the resource.

    Returns:
        A list of warnings.

    """
    warning_list: WarningList = WarningList()
    try:
        if isinstance(data, dict):
            validation_cls.model_validate(data, strict=True)
        elif isinstance(data, list):
            TypeAdapter(list[validation_cls]).validate_python(data)  # type: ignore[valid-type]
        else:
            raise ValueError(f"Expected a dictionary or list of dictionaries, got {type(data)}.")
    except ValidationError as e:
        printable_errors = tuple(humanize_validation_error(e))
        if printable_errors:
            warning_list.append(ResourceFormatWarning(source_file, printable_errors))
    return warning_list


def instantiate_class(
    data: dict[str, Any], validation_cls: type[T_BaseModel], source_file: Path, strict: bool = False
) -> T_BaseModel | ResourceFormatWarning:
    """Instantiates a class from a dictionary using the given pydantic model.

    Args:
        data: The data to instantiate the class from.
        validation_cls: The pydantic model to use for instantiation.
        source_file: The source file of the resource.
        strict: Whether to enforce types strictly.

    Returns:
        The instantiated class or a ResourceFormatWarning if validation failed.
    """
    try:
        return validation_cls.model_validate(data, strict=strict)
    except ValidationError as e:
        return ResourceFormatWarning(source_file, tuple(humanize_validation_error(e)))


def humanize_validation_error(error: ValidationError) -> list[str]:
    """Converts a ValidationError to a human-readable format.

    This overwrites the default error messages from Pydantic to be better suited for Toolkit users.

    Args:
        error: The ValidationError to convert.

    Returns:
        A list of human-readable error messages.
    """
    errors: list[str] = []
    item: ErrorDetails

    for item in error.errors(include_input=True, include_url=False):
        loc = item["loc"]
        error_type = item["type"]
        is_metadata_string_value_error = error_type == "string_type" and len(loc) >= 2 and loc[-2] == "metadata"
        if error_type == "missing":
            msg = f"Missing required field: {loc[-1]!r}"
        elif error_type == "extra_forbidden":
            msg = f"Unused field: {loc[-1]!r}"
        elif error_type == "value_error":
            msg = str(item["ctx"]["error"])
        elif error_type in {"literal_error", "list_type"}:
            msg = f"{item['msg']}. Got {item['input']!r}."
        elif is_metadata_string_value_error:
            # We skip metadata string errors. There are multiple reasons for this
            # 1. We often allow non-string metadata values, and parse them to string later. For example, in
            #     ExtractionPipelines.
            # 2. The user often set metadata values to int/bool/float by mistake, but the server accepts these values and
            #     converts them to string. Thus, these are not really errors.
            # 3. The metadata errors flood the output and obscure more important errors, and we see example of
            #     users ignoring all errors because of this (error fatigue).
            continue
        elif error_type == "string_type":
            msg = f"{item['msg']}. Got {item['input']!r} of type {type(item['input']).__name__}. Hint: Use double quotes to force string."
        elif error_type == "model_type":
            model_name = item["ctx"].get("class_name", "unknown")
            msg = f"Input must be an object of type {model_name}. Got {item['input']!r} of type {type(item['input']).__name__}."
        elif error_type in {
            "int_type",
            "bool_type",
            "datetime_type",
            "decimal_type",
            "float_type",
            "time_type",
            "timedelta_type",
            "dict_type",
        }:
            msg = f"{item['msg']}. Got {item['input']!r} of type {type(item['input']).__name__}."
        elif error_type == "union_tag_not_found" and "ctx" in item and "discriminator" in item["ctx"]:
            # This is when we use a discriminator field to determine the type in a union. For the user, this means they
            # are missing a required field.
            msg = f"Missing required field: {item['ctx']['discriminator']}"
        else:
            # Default to the Pydantic error message
            msg = item["msg"]

        if error_type.endswith("dict_type") and len(loc) > 1:
            # If this is a dict_type error for a JSON field, the location will be:
            #  dict[str,json-or-python[json=any,python=tagged-union[list[...],dict[str,...],str,bool,int,float,none]]]
            #  This is hard to read, so we simplify it to just the field name.
            loc = tuple(["dict" if isinstance(x, str) and "json-or-python" in x else x for x in loc])

        if len(loc) > 1 and error_type in {"extra_forbidden", "missing"}:
            # We skip the last element as this is in the message already
            msg = f"In {as_json_path(loc[:-1])} {msg[:1].casefold()}{msg[1:]}"
        elif len(loc) > 1:
            msg = f"In {as_json_path(loc)} {msg[:1].casefold()}{msg[1:]}"
        elif len(loc) == 1 and isinstance(loc[0], str) and error_type not in {"extra_forbidden", "missing"}:
            msg = f"In field {loc[0]} {msg[:1].casefold()}{msg[1:]}"
        errors.append(msg)
    return errors


def as_json_path(loc: tuple[str | int, ...]) -> str:
    """Converts a location tuple to a JSON path.

    Args:
        loc: The location tuple to convert.

    Returns:
        A JSON path string.
    """
    if not loc:
        return ""
    # +1 to convert from 0-based to 1-based indexing
    prefix = ""
    if isinstance(loc[0], int):
        prefix = "item "

    suffix = ".".join([str(x) if isinstance(x, str) else f"[{x + 1}]" for x in loc]).replace(".[", "[")
    return f"{prefix}{suffix}"


def validate_module_selection(
    modules: ModuleDirectories,
    config: BuildConfigYAML,
    packages: dict[str, list[str]],
    selected_modules: set[str | Path],
    organization_dir: Path,
) -> WarningList:
    """Validates module selection and returns warnings for non-critical issues.

    Critical errors (duplicate modules, missing modules, no modules selected) are still raised
    as exceptions as they prevent the build from proceeding.
    """
    warnings: WarningList = WarningList()

    # Validations: Ambiguous selection.
    selected_names = {s for s in config.environment.selected if isinstance(s, str)}
    if duplicate_modules := {
        module_name: paths
        for module_name, paths in modules.as_path_by_name().items()
        if len(paths) > 1 and module_name in selected_names
    }:
        # If the user has selected a module by name, and there are multiple modules with that name, raise an error.
        # Note, if the user uses a path to select a module, this error will not be raised.
        raise ToolkitDuplicatedModuleError(
            f"Ambiguous module selected in config.{config.environment.name}.yaml:", duplicate_modules
        )

    # Package Referenced Modules Exists
    for package, package_modules in packages.items():
        if package not in selected_names:
            # We do not check packages that are not selected.
            # Typically, the user will delete the modules that are irrelevant for them;
            # thus we only check the selected packages.
            continue
        if missing_packages := set(package_modules) - modules.available_names:
            raise ToolkitMissingModuleError(
                f"Package {package} defined in {CDFToml.file_name!s} is referring "
                f"the following missing modules {missing_packages}."
            )

    # Selected modules does not exists
    if missing_modules := set(selected_modules) - modules.available:
        hint = ModuleDefinition.long(missing_modules, organization_dir)
        raise ToolkitMissingModuleError(
            f"The following selected modules are missing, please check path: {missing_modules}.\n{hint}"
        )

    # Nothing is Selected
    if not modules.selected:
        raise ToolkitEnvError(
            f"No selected modules specified in {config.filepath!s}, have you configured "
            f"the environment ({config.environment.name})?"
        )

    # Dev modules warning (non-critical)
    dev_modules = modules.available_names & DEV_ONLY_MODULES
    if dev_modules and config.environment.validation_type != "dev":
        warnings.append(
            MediumSeverityWarning(
                "The following modules should [bold]only[/bold] be used a in CDF Projects designated as dev (development): "
                f"{humanize_collection(dev_modules)!r}",
            )
        )

    return warnings
