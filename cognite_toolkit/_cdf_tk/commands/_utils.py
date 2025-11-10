from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.cruds._base_cruds import T_ID


def _print_ids_or_length(resource_ids: SequenceNotStr[T_ID], limit: int = 10) -> str:
    if len(resource_ids) == 1:
        return f"{resource_ids[0]!r}"
    elif len(resource_ids) <= limit:
        return f"{resource_ids}"
    else:
        return f"{len(resource_ids)} items"
