from collections import defaultdict, deque
from collections.abc import Callable, Hashable
from typing import Any


def diff_list_hashable(local: list[Hashable], cdf: list[Hashable]) -> tuple[dict[int, int], list[int]]:
    """Matches items in the local list to items in the CDF list.

    Matching is one-to-one: each local item is matched to at most one CDF item and vice versa.
    Duplicates are matched in order of appearance.

    Returns:
        A mapping from local index to CDF index for matched items, and the CDF indices
        of items that have no match in the local list.
    """
    local_by_cdf: dict[int, int] = {}
    added: list[int] = []
    unmatched_local_indices: dict[Hashable, deque[int]] = defaultdict(deque)
    for i, item in enumerate(local):
        unmatched_local_indices[item].append(i)
    for index, item in enumerate(cdf):
        candidates = unmatched_local_indices.get(item)
        if candidates:
            local_by_cdf[candidates.popleft()] = index
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
    return hash(_freeze(item))


def _freeze(value: Any) -> Hashable:
    """Converts a (nested) structure of dicts and lists into a canonical hashable representation.

    Dict key order does not matter, while list order does. Type markers ensure that, for example,
    an empty dict and an empty list are distinguished.
    """
    if isinstance(value, dict):
        return "dict", tuple(sorted(((key, _freeze(val)) for key, val in value.items()), key=lambda x: x[0]))
    if isinstance(value, list):
        return "list", tuple(_freeze(item) for item in value)
    if isinstance(value, bool):
        # bool is a subclass of int, and True == 1, so it must be distinguished explicitly.
        return "bool", value
    if isinstance(value, Hashable):
        return value
    raise ValueError(f"Cannot hash value {value}")


def hash_dict(d: dict) -> int:
    return hash(_freeze(d))


def hash_list(lst: list) -> int:
    return hash(_freeze(lst))


def dm_identifier(data: dict[str, Any]) -> tuple[str, ...]:
    return data.get("type", ""), data["space"], data["externalId"], data.get("version", "")
