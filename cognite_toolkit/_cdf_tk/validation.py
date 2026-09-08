from pathlib import Path
from typing import Any, NamedTuple, TypeVar

from pydantic import BaseModel, TypeAdapter, ValidationError
from pydantic_core import ErrorDetails

from cognite_toolkit._cdf_tk.tk_warnings import (
    WarningList,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.yaml_classes import BaseModelResource

__all__ = [
    "humanize_validation_error",
    "humanize_validation_error_categorized",
]


T_BaseModel = TypeVar("T_BaseModel", bound=BaseModel)


class _MessageEntry(NamedTuple):
    message: str
    category: str


class _GroupEntry(NamedTuple):
    loc: tuple[str | int, ...]


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

    Multiple missing fields at the same location are grouped into a single "missing required fields" message,
    and likewise for multiple unknown fields, instead of repeating the location for each individual field.

    Args:
        error: The ValidationError to convert.

    Returns:
        A list of human-readable error messages.
    """
    return [message for message, _ in humanize_validation_error_categorized(error)]


def humanize_validation_error_categorized(error: ValidationError) -> list[tuple[str, str]]:
    """Same as ``humanize_validation_error``, but also classifies each message as "error" or "warning".

    Unrecognized fields and invalid enum/literal values are classified as "warning" since they do not
    prevent the resource from being built or deployed. Everything else is classified as "error".

    Args:
        error: The ValidationError to convert.

    Returns:
        A list of (message, category) tuples, where category is either "error" or "warning".
    """

    # Ordered list of either a (literal message, category) entry, or a group entry referring into `field_groups_by_loc`.
    ordered_entries: list[_MessageEntry | _GroupEntry] = []
    field_groups_by_loc: dict[tuple[str | int, ...], dict[str, list[str]]] = {}
    item: ErrorDetails

    for item in error.errors(include_input=True, include_url=False):
        loc = item["loc"]
        error_type = item["type"]
        category = "error"
        is_metadata_string_value_error = error_type == "string_type" and len(loc) >= 2 and loc[-2] == "metadata"
        # A nested object field left empty in YAML (e.g. "view:" with nothing indented under it) is
        # reported by Pydantic as "model_type" with a None input. The field is present but empty, which
        # is often caused by its properties being under-indented so they end up as siblings instead.
        is_empty_nested_object = error_type == "model_type" and item["input"] is None
        if len(loc) > 1 and (error_type in {"missing", "extra_forbidden"} or is_empty_nested_object):
            group_loc = loc[:-1]
            group = field_groups_by_loc.setdefault(group_loc, {"missing": [], "unknown": [], "empty": []})
            if _GroupEntry(group_loc) not in ordered_entries:
                ordered_entries.append(_GroupEntry(group_loc))
            if error_type == "extra_forbidden":
                key = "unknown"
            elif is_empty_nested_object:
                key = "empty"
            else:
                key = "missing"
            group[key].append(f"{loc[-1]!r}")
            continue
        if error_type == "missing":
            msg = f"Missing required field: {loc[-1]!r}"
        elif error_type == "extra_forbidden":
            msg = f"Unknown field: {loc[-1]!r}"
            category = "warning"
        elif error_type == "value_error":
            msg = str(item["ctx"]["error"])
        elif error_type == "literal_error":
            expected = item.get("ctx", {}).get("expected", item["msg"].removeprefix("Input should be "))
            prefix = "Expected one of" if " or " in expected else "Expected"
            msg = f"{prefix} {expected}. Got {item['input']!r}."
            category = "warning"
        elif error_type == "list_type":
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
            msg = f"Input should be a valid {model_name} object. Got {item['input']!r} of type {type(item['input']).__name__}."
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

        if len(loc) >= 1 and error_type not in {"extra_forbidden", "missing"}:
            # Note: "missing"/"extra_forbidden" errors are handled above and never reach here.
            if error_type == "literal_error":
                # "Unrecognized value" always reads as "for a field", regardless of path depth.
                prefix, connector = "Unrecognized value", "for"
            else:
                prefix = "Invalid value"
                # A single top-level field name (e.g. "name") reads awkwardly with "at"; use "for" instead.
                # Nested or indexed paths (e.g. "settings.template.name", "actions[1]") still read fine with "at".
                connector = "for" if len(loc) == 1 and isinstance(loc[0], str) else "at"
            msg = f"{prefix} {connector} {as_json_path(loc)}: {msg}"
        ordered_entries.append(_MessageEntry(msg, category))

    errors: list[tuple[str, str]] = []
    for entry in ordered_entries:
        if isinstance(entry, _MessageEntry):
            errors.append((entry.message, entry.category))
            continue
        group = field_groups_by_loc[entry.loc]
        path = as_json_path(entry.loc)
        if missing := group["missing"]:
            field_word = "field" if len(missing) == 1 else "fields"
            errors.append((f"Missing required {field_word} in {path}: {humanize_collection(missing)}", "error"))
        if empty := group["empty"]:
            field_word = "field" if len(empty) == 1 else "fields"
            errors.append(
                (
                    f"Empty {field_word} in {path}: {humanize_collection(empty)}. "
                    "Hint: Check that its properties are properly indented underneath it.",
                    "error",
                )
            )
        if unknown := group["unknown"]:
            field_word = "field" if len(unknown) == 1 else "fields"
            errors.append((f"Unrecognized {field_word} in {path}: {humanize_collection(unknown)}. ", "warning"))
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
