from __future__ import annotations

import inspect
import re
from pathlib import Path
from typing import Any

from cognite.client.data_classes._base import CogniteObject
from cognite.client.utils._text import to_camel_case, to_snake_case

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet, read_parameters_from_dict
from cognite_toolkit._cdf_tk.loaders import NodeLoader
from cognite_toolkit._cdf_tk.tk_warnings import (
    CaseTypoWarning,
    DataSetMissingWarning,
    MissingRequiredParameterWarning,
    TemplateVariableWarning,
    UnusedParameterWarning,
    WarningList,
)

__all__ = ["validate_modules_variables", "validate_data_set_is_set", "validate_resource_yaml"]


def validate_modules_variables(config: dict[str, Any], filepath: Path, path: str = "") -> WarningList:
    """Checks whether the config file has any issues.

    Currently, this checks for:
        * Non-replaced template variables, such as <change_me>.

    Args:
        config: The config to check.
        filepath: The filepath of the config.yaml.
        path: The path in the config.yaml. This is used recursively by this function.
    """
    warning_list: WarningList = WarningList()
    pattern = re.compile(r"<.*?>")
    for key, value in config.items():
        if isinstance(value, str) and pattern.match(value):
            warning_list.append(TemplateVariableWarning(filepath, value, key, path))
        elif isinstance(value, dict):
            if path:
                path += "."
            warning_list.extend(validate_modules_variables(value, filepath, f"{path}{key}"))
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
