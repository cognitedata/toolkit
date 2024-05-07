from __future__ import annotations

import abc
import collections
import inspect
import re
import sys
import types
import typing
from pathlib import Path
from typing import Any, get_origin

from cognite.client.data_classes._base import CogniteObject
from cognite.client.utils._text import to_camel_case, to_snake_case

from cognite_toolkit._cdf_tk._get_type_hints import _TypeHints

from .warning.fileread import (
    DataSetMissingWarning,
    DataSetMissingWarningList,
    SnakeCaseWarning,
    SnakeCaseWarningList,
    TemplateVariableWarning,
    TemplateVariableWarningList,
)


def validate_case_raw(
    raw: dict[str, Any] | list[dict[str, Any]],
    resource_cls: type[CogniteObject],
    filepath: Path,
    identifier_key: str = "externalId",
) -> SnakeCaseWarningList:
    """Checks whether camel casing the raw data would match a parameter in the resource class.

    Args:
        raw: The raw data to check.
        resource_cls: The resource class to check against init method
        filepath: The filepath of the raw data. This is used to pass to the warnings for easy
            grouping of warnings.
        identifier_key: The key to use as identifier. Defaults to "externalId". This is used to pass to the warnings
            for easy grouping of warnings.

    Returns:
        A list of CaseWarning objects.

    """
    return _validate_case_raw(raw, resource_cls, filepath, identifier_key)


def _validate_case_raw(
    raw: dict[str, Any] | list[dict[str, Any]],
    resource_cls: type[CogniteObject],
    filepath: Path,
    identifier_key: str = "externalId",
    identifier_value: str = "",
) -> SnakeCaseWarningList:
    warnings = SnakeCaseWarningList()
    if isinstance(raw, list):
        for item in raw:
            warnings.extend(_validate_case_raw(item, resource_cls, filepath, identifier_key))
        return warnings
    elif not isinstance(raw, dict):
        return warnings

    signature = inspect.signature(resource_cls.__init__)

    is_base_class = inspect.isclass(resource_cls) and any(base is abc.ABC for base in resource_cls.__bases__)
    if is_base_class:
        # If it is a base class, it cannot be instantiated, so it can be any of the
        # subclasses' parameters.
        expected = {
            to_camel_case(parameter)
            for sub in resource_cls.__subclasses__()
            for parameter in inspect.signature(sub.__init__).parameters.keys()
        } - {"self"}
    else:
        expected = set(map(to_camel_case, signature.parameters.keys())) - {"self"}

    actual = set(raw.keys())
    actual_camel_case = set(map(to_camel_case, actual))
    snake_cased = actual - actual_camel_case

    if not identifier_value:
        identifier_value = raw.get(
            identifier_key, raw.get(to_snake_case(identifier_key), f"No identifier {identifier_key}")
        )

    for key in snake_cased:
        if (camel_key := to_camel_case(key)) in expected:
            warnings.append(SnakeCaseWarning(filepath, identifier_value, identifier_key, str(key), str(camel_key)))

    try:
        type_hints_by_name = _TypeHints.get_type_hints_by_name(signature, resource_cls)
    except Exception:
        # If we cannot get type hints, we cannot check if the type is correct.
        return warnings

    for key, value in raw.items():
        if not isinstance(value, dict):
            continue
        if (parameter := signature.parameters.get(to_snake_case(key))) and (
            type_hint := type_hints_by_name.get(parameter.name)
        ):
            if inspect.isclass(type_hint) and issubclass(type_hint, CogniteObject):
                warnings.extend(_validate_case_raw(value, type_hint, filepath, identifier_key, identifier_value))
                continue

            container_type = get_origin(type_hint)
            if sys.version_info >= (3, 10):
                # UnionType was introduced in Python 3.10
                if container_type is types.UnionType:
                    args = typing.get_args(type_hint)
                    type_hint = next((arg for arg in args if arg is not type(None)), None)

            mappings = [dict, collections.abc.MutableMapping, collections.abc.Mapping]
            is_mapping = container_type in mappings or (
                isinstance(type_hint, types.GenericAlias) and len(typing.get_args(type_hint)) == 2
            )
            if not is_mapping:
                continue
            args = typing.get_args(type_hint)
            if not args:
                continue
            container_key, container_value = args
            if inspect.isclass(container_value) and issubclass(container_value, CogniteObject):
                for sub_key, sub_value in value.items():
                    warnings.extend(
                        _validate_case_raw(sub_value, container_value, filepath, identifier_key, identifier_value)
                    )

    return warnings


def validate_modules_variables(config: dict[str, Any], filepath: Path, path: str = "") -> TemplateVariableWarningList:
    """Checks whether the config file has any issues.

    Currently, this checks for:
        * Non-replaced template variables, such as <change_me>.

    Args:
        config: The config to check.
        filepath: The filepath of the config.yaml.
        path: The path in the config.yaml. This is used recursively by this function.
    """
    warnings = TemplateVariableWarningList()
    pattern = re.compile(r"<.*?>")
    for key, value in config.items():
        if isinstance(value, str) and pattern.match(value):
            warnings.append(TemplateVariableWarning(filepath, value, key, path))
        elif isinstance(value, dict):
            if path:
                path += "."
            warnings.extend(validate_modules_variables(value, filepath, f"{path}{key}"))
    return warnings


def validate_data_set_is_set(
    raw: dict[str, Any] | list[dict[str, Any]],
    resource_cls: type[CogniteObject],
    filepath: Path,
    identifier_key: str = "externalId",
) -> DataSetMissingWarningList:
    warnings = DataSetMissingWarningList()
    signature = inspect.signature(resource_cls.__init__)
    if "data_set_id" not in set(signature.parameters.keys()):
        return warnings

    if isinstance(raw, list):
        for item in raw:
            warnings.extend(validate_data_set_is_set(item, resource_cls, filepath, identifier_key))
        return warnings

    if "dataSetExternalId" in raw or "dataSetId" in raw:
        return warnings

    value = raw.get(identifier_key, raw.get(to_snake_case(identifier_key), f"No identifier {identifier_key}"))
    warnings.append(DataSetMissingWarning(filepath, value, identifier_key, resource_cls.__name__))
    return warnings
