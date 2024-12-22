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
