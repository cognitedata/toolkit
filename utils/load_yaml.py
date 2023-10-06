"""
Module containing a YAML loader to automatically serialize these
dataclasses from a config file.
"""
# mypy: ignore-errors
import json
import os
import re
from enum import Enum
from hashlib import sha256
from typing import Any, Callable, Dict, Iterable, TextIO, Type, TypeVar, Union

import dacite
import yaml
from yaml.scanner import ScannerError

CustomConfigClass = TypeVar("CustomConfigClass")


class InvalidConfigError(Exception):
    """
    Exception thrown from ``load_yaml`` if config file is invalid. This can be due to

      * Missing fields
      * Incompatible types
      * Unkown fields
    """

    def __init__(self, message: str):
        super(InvalidConfigError, self).__init__()
        self.message = message

    def __str__(self) -> str:
        return f"Invalid config: {self.message}"

    def __repr__(self) -> str:
        return self.__str__()


def _to_snake_case(dictionary: Dict[str, Any], case_style: str) -> Dict[str, Any]:
    """
    Ensure that all keys in the dictionary follows the snake casing convention (recursively, so any sub-dictionaries are
    changed too).

    Args:
        dictionary: Dictionary to update.
        case_style: Existing casing convention. Either 'snake', 'hyphen' or 'camel'.

    Returns:
        An updated dictionary with keys in the given convention.
    """

    def fix_list(list_, key_translator):
        if list_ is None:
            return []

        new_list = [None] * len(list_)
        for i, element in enumerate(list_):
            if isinstance(element, dict):
                new_list[i] = fix_dict(element, key_translator)
            elif isinstance(element, list):
                new_list[i] = fix_list(element, key_translator)
            else:
                new_list[i] = element
        return new_list

    def fix_dict(dict_, key_translator):
        if dict_ is None:
            return {}

        new_dict = {}
        for key in dict_:
            if isinstance(dict_[key], dict):
                new_dict[key_translator(key)] = fix_dict(dict_[key], key_translator)
            elif isinstance(dict_[key], list):
                new_dict[key_translator(key)] = fix_list(dict_[key], key_translator)
            else:
                new_dict[key_translator(key)] = dict_[key]
        return new_dict

    def translate_hyphen(key):
        return key.replace("-", "_")

    def translate_camel(key):
        return re.sub(r"([A-Z]+)", r"_\1", key).strip("_").lower()

    if case_style == "snake" or case_style == "underscore":
        return dictionary
    elif case_style == "hyphen" or case_style == "kebab":
        return fix_dict(dictionary, translate_hyphen)
    elif case_style == "camel" or case_style == "pascal":
        return fix_dict(dictionary, translate_camel)
    else:
        raise ValueError(f"Invalid case style: {case_style}")


def _load_yaml(
    source: Union[TextIO, str],
    config_type: Type[CustomConfigClass],
    case_style: str = "hyphen",
    expand_envvars=True,
    dict_manipulator: Callable[[Dict[str, Any]], Dict[str, Any]] = lambda x: x,
) -> CustomConfigClass:
    def env_constructor(_: yaml.SafeLoader, node):
        bool_values = {
            "true": True,
            "false": False,
        }
        expanded_value = os.path.expandvars(node.value)
        return bool_values.get(expanded_value.lower(), expanded_value)

    class EnvLoader(yaml.SafeLoader):
        pass

    EnvLoader.add_implicit_resolver("!env", re.compile(r"\$\{([^}^{]+)\}"), None)
    EnvLoader.add_constructor("!env", env_constructor)

    loader = EnvLoader if expand_envvars else yaml.SafeLoader

    # Safe to use load instead of safe_load since both loader classes are based on SafeLoader
    try:
        config_dict = yaml.load(source, Loader=loader)
    except ScannerError as e:
        location = e.problem_mark or e.context_mark
        formatted_location = (
            f" at line {location.line+1}, column {location.column+1}"
            if location is not None
            else ""
        )
        cause = e.problem or e.context
        raise InvalidConfigError(
            f"Invalid YAML{formatted_location}: {cause or ''}"
        ) from e

    config_dict = dict_manipulator(config_dict)
    config_dict = _to_snake_case(config_dict, case_style)

    try:
        config = dacite.from_dict(
            data=config_dict,
            data_class=config_type,
            config=dacite.Config(strict=True, cast=[Enum]),
        )
    except dacite.UnexpectedDataError as e:
        unknowns = [
            f'"{k.replace("_", "-") if case_style == "hyphen" else k}"' for k in e.keys
        ]
        raise InvalidConfigError(
            f"Unknown config parameter{'s' if len(unknowns) > 1 else ''} {', '.join(unknowns)}"
        )

    except (
        dacite.WrongTypeError,
        dacite.MissingValueError,
        dacite.UnionMatchError,
    ) as e:
        path = (
            e.field_path.replace("_", "-") if case_style == "hyphen" else e.field_path
        )

        def name(type_: Type) -> str:
            return type_.__name__ if hasattr(type_, "__name__") else str(type_)

        def all_types(type_: Type) -> Iterable[Type]:
            return type_.__args__ if hasattr(type_, "__args__") else [type_]

        if (
            isinstance(e, (dacite.WrongTypeError, dacite.UnionMatchError))
            and e.value is not None
        ):
            got_type = name(type(e.value))
            need_type = ", ".join(name(t) for t in all_types(e.field_type))

            raise InvalidConfigError(
                f'Wrong type for field "{path}" - got "{e.value}" of type {got_type} instead of {need_type}'
            )
        raise InvalidConfigError(f'Missing mandatory field "{path}"')

    except dacite.ForwardReferenceError as e:
        raise ValueError(f"Invalid config class: {str(e)}")

    config._file_hash = sha256(json.dumps(config_dict).encode("utf-8")).hexdigest()

    return config


def load_yaml(
    source: Union[TextIO, str],
    config_type: Type[CustomConfigClass],
    case_style: str = "hyphen",
    expand_envvars=True,
) -> CustomConfigClass:
    """
    Read a YAML file, and create a config object based on its contents.

    Args:
        source: Input stream (as returned by open(...)) or string containing YAML.
        config_type: Class of config type.
        case_style: Casing convention of config file. Valid options are 'snake', 'hyphen' or 'camel'. Should be
            'hyphen'.
        expand_envvars: Substitute values with the pattern ${VAR} with the content of the environment variable VAR

    Returns:
        An initialized config object.

    Raises:
        InvalidConfigError: If any config field is given as an invalid type, is missing or is unknown
    """
    return _load_yaml(
        source=source,
        config_type=config_type,
        case_style=case_style,
        expand_envvars=expand_envvars,
    )


T = TypeVar("T")
