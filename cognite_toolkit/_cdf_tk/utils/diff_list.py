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


def hash_dict(d: dict) -> int:
    hash_ = 0
    for key, value in sorted(d.items(), key=lambda x: x[0]):
        if isinstance(value, dict):
            hash_ ^= hash_dict(value)
        elif isinstance(value, list):
            hash_ ^= hash_list(value)
        elif isinstance(value, Hashable):
            hash_ ^= hash((key, value))
        else:
            raise ValueError(f"Cannot hash value {value}")
    return hash_


def hash_list(lst: list) -> int:
    hash_ = 0
    for i, item in enumerate(lst):
        if isinstance(item, dict):
            hash_ ^= hash_dict(item)
        elif isinstance(item, list):
            hash_ ^= hash_list(item)
        elif isinstance(item, Hashable):
            hash_ ^= hash((i, item))
        else:
            raise ValueError(f"Cannot hash value {item}")
    return hash_
