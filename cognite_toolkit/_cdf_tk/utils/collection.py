import difflib
from collections.abc import Collection, Iterable, Iterator, Sequence, Set
from itertools import islice
from typing import Any, TypeVar

from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump


def flatten_dict(dct: dict[str, Any]) -> dict[tuple[str, ...], Any]:
    """Flatten a dictionary to a list of tuples with the key path and value."""
    items: dict[tuple[str, ...], Any] = {}
    for key, value in dct.items():
        if isinstance(value, dict):
            for sub_key, sub_value in flatten_dict(value).items():
                items[(key, *sub_key)] = sub_value
        else:
            items[(key,)] = value
    return items


def flatten_dict_json_path(dct: dict[str, Any], keep_structured: Set[str] | None = None) -> dict[str, Any]:
    """Flatten a dictionary to a dictionary with JSON path keys.

    Empty keys are ignored.

    Args:
        dct: The dictionary to flatten.
        keep_structured: A set of keys to keep structured (not flatten). If a key is in this set,
            it will not be flattened, and the value will be kept as is. This only applies to top-level keys.

    Returns:
        A dictionary with JSON path keys.
    """

    return _flatten(dct, keep_structured=keep_structured or set())


def _flatten(obj: Any, keep_structured: Set[str], path: str = "") -> dict[str, Any]:
    items: dict[str, Any] = {}
    if isinstance(obj, dict):
        for key, value in obj.items():
            if not key:
                continue
            current_path = f"{path}.{key}" if path else key
            if key in keep_structured:
                items[current_path] = value
            else:
                items.update(_flatten(value, set(), current_path))
    elif isinstance(obj, list):
        for i, value in enumerate(obj):
            current_path = f"{path}[{i}]"
            if isinstance(value, (dict, list)):
                items.update(_flatten(value, set(), current_path))
            else:
                items[current_path] = value
    else:
        items[path] = obj

    return items


def to_diff(a: dict[str, Any], b: dict[str, Any]) -> Iterator[str]:
    a_str = yaml_safe_dump(a, sort_keys=True)
    b_str = yaml_safe_dump(b, sort_keys=True)

    return difflib.unified_diff(a_str.splitlines(), b_str.splitlines())


def in_dict(keys: Iterable[str], dictionary: dict) -> bool:
    return all(key in dictionary for key in keys)


def humanize_collection(collection: Collection[Any], /, *, sort: bool = True, bind_word: str = "and") -> str:
    if not collection:
        return ""
    elif len(collection) == 1:
        return str(next(iter(collection)))

    strings = (str(item) for item in collection)
    if sort:
        sequence = sorted(strings)
    else:
        sequence = list(strings)

    return f"{', '.join(sequence[:-1])} {bind_word} {sequence[-1]}"


def chunker(iterable: Iterable[Any], size: int) -> Iterator[list[Any]]:
    iterator = iter(iterable)
    while chunk := list(islice(iterator, size)):
        yield chunk


T_Sequence = TypeVar("T_Sequence", bound=Sequence[Any])


def chunker_sequence(sequence: T_Sequence, size: int) -> Iterator[T_Sequence]:
    """Yield successive n-sized chunks from sequence."""
    for i in range(0, len(sequence), size):
        # Slicing does not preserve a TypeVar bound to Sequence. Sequences such as
        # MigrationMappingList still return their own type at runtime.
        yield sequence[i : i + size]  # type: ignore[misc]  # ty: ignore[invalid-yield]


def get_string_nested_dict(dct: dict[str, Any], path: tuple[str | int, ...]) -> str | None:
    """Get a string value from a nested dictionary using a path of keys.

    Finds the value at the specified path in the nested dictionary.

    Returns:
        The string value at the specified path, or None if the path does not exist or the value is not a string.

    Raises:
        TypeError: If the value at the specified path is not a string.
    """
    current: Any = dct
    for key in path:
        if isinstance(current, dict) and key in current and isinstance(key, str):
            current = current[key]
        elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
            current = current[key]
        elif isinstance(key, str) and not isinstance(current, dict):
            raise TypeError(f"Expected a dictionary at path {path}, but found {type(current).__name__}")
        elif isinstance(key, int) and not isinstance(current, list):
            raise TypeError(f"Expected a list at path {path}, but found {type(current).__name__}")
        else:
            return None

    if isinstance(current, str) or current is None:
        return current
    else:
        raise TypeError(f"Value at path {path} is not a string: {current}")


def set_nested_dict_if_path_exists(dct: dict[str, Any], path: tuple[str | int, ...], value: Any) -> None:
    """Set a value in a nested dictionary using a path of keys, if the path exists.

    Args:
        dct: The nested dictionary to modify.
        path: A tuple of keys representing the path to the value to set.
        value: The value to set at the specified path.

    Raises:
        TypeError: If the value at the specified path is not a dictionary or list when expected
    """
    current: Any = dct
    for key in path[:-1]:
        if isinstance(current, dict) and key in current and isinstance(key, str):
            current = current[key]
        elif isinstance(current, list) and isinstance(key, int) and 0 <= key < len(current):
            current = current[key]
        else:
            return  # Path does not exist, do nothing

    last_key = path[-1]
    if isinstance(current, dict) and isinstance(last_key, str):
        if last_key in current:
            current[last_key] = value
    elif isinstance(current, list) and isinstance(last_key, int) and 0 <= last_key < len(current):
        current[last_key] = value
    else:
        raise TypeError(f"Cannot set value at path {path}: expected a dictionary or list at the last key.")
