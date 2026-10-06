from collections.abc import Callable, Hashable
from typing import Any


def diff_list_hashable(local: list[Hashable], cdf: list[Hashable]) -> tuple[dict[int, int], list[int]]:
    local_by_cdf: dict[int, int] = {}
    added: list[int] = []
    index_by_local = {item: i for i, item in enumerate(local)}
    for index, item in enumerate(cdf):
        if item in index_by_local:
            local_by_cdf[index_by_local[item]] = index
        else:
            added.append(index)
    return local_by_cdf, added


def diff_list_identifiable(
    local: list[Any], cdf: list[Any], *, get_identifier: Callable[[Any], Hashable]
) -> tuple[dict[int, int], list[int]]:
    return diff_list_hashable([get_identifier(item) for item in local], [get_identifier(item) for item in cdf])


def diff_list_force_hashable(local: Any, cdf: Any) -> tuple[dict[int, int], list[int]]:
    return diff_list_identifiable(local, cdf, get_identifier=force_hash)


def force_hash(item: Any) -> int:
    if isinstance(item, dict):
        return hash_dict(item)
    if isinstance(item, list):
        return hash_list(item)
    if isinstance(item, Hashable):
        return hash(item)
    raise ValueError(f"Cannot hash value {item}")


def _freeze(value: Any) -> Hashable:
    """Converts a (nested) structure of dicts and lists into a canonical hashable representation.

    Dict key order does not matter, while list order does. Type markers ensure that, for example,
    an empty dict and an empty list are distinguished.
    """
    if isinstance(value, dict):
        return "dict", tuple(sorted(((key, _freeze(val)) for key, val in value.items()), key=lambda x: x[0]))
    if isinstance(value, list):
        return "list", tuple(_freeze(item) for item in value)
    if isinstance(value, Hashable):
        return value
    raise ValueError(f"Cannot hash value {value}")


def hash_dict(d: dict) -> int:
    return hash(_freeze(d))


def hash_list(lst: list) -> int:
    return hash(_freeze(lst))


def dm_identifier(data: dict[str, Any]) -> tuple[str, ...]:
    return data.get("type", ""), data["space"], data["externalId"], data.get("version", "")
