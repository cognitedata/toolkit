import difflib
from collections.abc import Collection, Iterable, Iterator
from itertools import islice
from typing import Any

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
