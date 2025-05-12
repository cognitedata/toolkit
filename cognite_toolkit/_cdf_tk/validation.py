from __future__ import annotations

import inspect
import re
from pathlib import Path
from typing import Any

from cognite.client.data_classes._base import CogniteObject
from cognite.client.utils._text import to_camel_case, to_snake_case
from pydantic import TypeAdapter, ValidationError
from pydantic_core import ErrorDetails

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet, read_parameters_from_dict
from cognite_toolkit._cdf_tk.data_classes import BuildVariables
from cognite_toolkit._cdf_tk.loaders import NodeLoader
from cognite_toolkit._cdf_tk.resource_classes import ToolkitResource
from cognite_toolkit._cdf_tk.tk_warnings import (
    CaseTypoWarning,
    DataSetMissingWarning,
    MissingRequiredParameterWarning,
    TemplateVariableWarning,
    UnusedParameterWarning,
    WarningList,
)

__all__ = ["validate_data_set_is_set", "validate_modules_variables", "validate_resource_yaml"]

from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning


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


def validate_resource_yaml(
    data: dict | list, spec: ParameterSpecSet, source_file: Path, element: int | None = None
) -> WarningList:
    if spec.spec_name == NodeLoader.__name__:
        # Special case for NodeLoader as it has options for API call parameters
        if isinstance(data, list):
            return _validate_resource_yaml(data, spec, source_file)
        elif isinstance(data, dict) and "node" in data:
            return _validate_resource_yaml(data["node"], spec, source_file)
        elif isinstance(data, dict) and "nodes" in data:
            return _validate_resource_yaml(data["nodes"], spec, source_file)
        else:
            return _validate_resource_yaml(data, spec, source_file)
    else:
        return _validate_resource_yaml(data, spec, source_file, element)


def _validate_resource_yaml(
    data: dict | list, spec: ParameterSpecSet, source_file: Path, element: int | None = None
) -> WarningList:
    warnings: WarningList = WarningList()
    if isinstance(data, list):
        for no, item in enumerate(data, 1):
            warnings.extend(_validate_resource_yaml(item, spec, source_file, no))
        return warnings
    elif not isinstance(data, dict):
        raise NotImplementedError("Note: This function only supports top-level and lists dictionaries.")

    actual_parameters = read_parameters_from_dict(data)
    unused_parameters = actual_parameters - spec
    unused_cased = unused_parameters.as_camel_case() - spec
    typo_parameters = unused_parameters - unused_cased
    for parameter in typo_parameters:
        key = parameter.key
        warnings.append(CaseTypoWarning(source_file, element, parameter.path, key, to_camel_case(key)))

    unused_parameters = unused_parameters - typo_parameters
    for parameter in unused_parameters:
        key = parameter.key
        warnings.append(UnusedParameterWarning(source_file, element, parameter.path, key))

    # Only checking the top level for now. This can be expanded to check nested parameters.
    missing = spec.required(level=1) - actual_parameters
    for spec_param in missing:
        warnings.append(MissingRequiredParameterWarning(source_file, element, spec_param.path, spec_param.key))

    return warnings


def validate_resource_yaml_pydantic(
    data: dict[str, object] | list[dict[str, object]], validation_cls: type[ToolkitResource], source_file: Path
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
    if isinstance(data, dict):
        try:
            validation_cls.model_validate(data)
        except ValidationError as e:
            warning_list.append(ResourceFormatWarning(source_file, tuple(_humanize_validation_error(e))))
    elif isinstance(data, list):
        try:
            TypeAdapter(list[validation_cls]).validate_python(data)  # type: ignore[valid-type]
        except ValidationError as e:
            warning_list.append(ResourceFormatWarning(source_file, tuple(_humanize_validation_error(e))))
    else:
        raise ValueError(f"Expected a dictionary or list of dictionaries, got {type(data)}.")
    return warning_list


def _humanize_validation_error(error: ValidationError) -> list[str]:
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
        if error_type == "missing":
            msg = f"Missing required field: {loc[-1]!r}"
        elif error_type == "extra_forbidden":
            msg = f"Unused field: {loc[-1]!r}"
        elif error_type == "value_error":
            msg = str(item["ctx"]["error"])
        else:
            # Default to the Pydantic error message
            msg = item["msg"]
        if len(loc) > 1:
            msg = f"In {as_json_path(loc[:-1])} {msg[0].casefold()}{msg[1:]}"
        errors.append(msg)
    return errors


def as_json_path(loc: tuple[str | int, ...]) -> str:
    """Converts a location tuple to a JSON path.

    Args:
        loc: The location tuple to convert.

    Returns:
        A JSON path string.
    """
    if len(loc) == 1 and isinstance(loc[0], int):
        return f"item [{loc[0]}]"

    return ".".join([str(x) if isinstance(x, str) else f"[{x}]" for x in loc]).replace(".[", "[")
