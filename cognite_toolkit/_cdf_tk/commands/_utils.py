from __future__ import annotations

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.loaders import (
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, T_WritableCogniteResourceList


def _print_ids_or_length(resource_ids: SequenceNotStr[T_ID], limit: int = 10) -> str:
    if len(resource_ids) == 1:
        return f"{resource_ids[0]!r}"
    elif len(resource_ids) <= limit:
        return f"{resource_ids}"
    else:
        return f"{len(resource_ids)} items"


def _remove_duplicates(
    loaded_resources: T_CogniteResourceList,
    loader: ResourceLoader[
        T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
    ],
) -> tuple[T_CogniteResourceList, list[T_ID]]:
    seen: set[T_ID] = set()
    output = loader.list_write_cls([])
    duplicates: list[T_ID] = []
    for item in loaded_resources:
        identifier = loader.get_id(item)
        if identifier not in seen:
            output.append(item)
            seen.add(identifier)
        else:
            duplicates.append(identifier)
    return output, duplicates
