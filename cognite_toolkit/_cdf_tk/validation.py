import inspect
import re
from pathlib import Path
from typing import Any, TypeVar

from cognite.client.data_classes._base import CogniteObject
from cognite.client.utils._text import to_camel_case, to_snake_case
from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic_core import ErrorDetails

from cognite_toolkit._cdf_tk._parameters import ParameterSpecSet, read_parameters_from_dict
from cognite_toolkit._cdf_tk.cruds import NodeCRUD
from cognite_toolkit._cdf_tk.data_classes import BuildVariables
from cognite_toolkit._cdf_tk.resource_classes import BaseModelResource
from cognite_toolkit._cdf_tk.tk_warnings import (
    CaseTypoWarning,
    DataSetMissingWarning,
    MissingRequiredParameterWarning,
    TemplateVariableWarning,
    UnusedParameterWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning

__all__ = ["validate_data_set_is_set", "validate_modules_variables", "validate_resource_yaml"]


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


def validate_resource_yaml(
    data: dict | list, spec: ParameterSpecSet, source_file: Path, element: int | None = None
) -> WarningList:
    if spec.spec_name == NodeCRUD.__name__:
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
    if isinstance(data, dict):
        try:
            validation_cls.model_validate(data, strict=True)
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
        return ResourceFormatWarning(source_file, tuple(_humanize_validation_error(e)))


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
