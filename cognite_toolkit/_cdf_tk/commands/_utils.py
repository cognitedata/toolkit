from collections.abc import Sequence

from cognite_toolkit._cdf_tk.client._resource_base import T_Identifier


def _print_ids_or_length(resource_ids: Sequence[T_Identifier,], limit: int = 10) -> str:
    if len(resource_ids) == 1:
        return f"{resource_ids[0]!r}"
    elif len(resource_ids) <= limit:
        return f"{resource_ids}"
    else:
        return f"{len(resource_ids)} items"
