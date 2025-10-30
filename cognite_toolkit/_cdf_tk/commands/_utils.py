import re
from typing import Any

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.cruds import (
    ResourceCRUD,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import T_ID, T_WritableCogniteResourceList


def _print_ids_or_length(resource_ids: SequenceNotStr[T_ID], limit: int = 10) -> str:
    if len(resource_ids) == 1:
        return f"{resource_ids[0]!r}"
    elif len(resource_ids) <= limit:
        return f"{resource_ids}"
    else:
        return f"{len(resource_ids)} items"


def _remove_duplicates(
    loaded_resources: T_CogniteResourceList,
    loader: ResourceCRUD[
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


def format_type_annotation(annotation: Any) -> str:
    """
    Format a type annotation to show simplified type names without full module paths.

    Examples:
        cognite_toolkit._cdf_tk.data_classes.search_config.ViewId -> search_config.ViewId
        list[cognite_toolkit._cdf_tk.data_classes.search_config.PropertyConfig] -> list[search_config.PropertyConfig]
        str -> str
    """
    # Convert annotation to string
    type_str = str(annotation)

    # Remove 'typing.' prefix for generic types
    type_str = type_str.replace("typing.", "")

    # Pattern to match full module paths like cognite_toolkit._cdf_tk.data_classes.search_config.ViewId
    # We want to keep only the last two parts (search_config.ViewId)
    pattern = r"cognite_toolkit\._cdf_tk\.data_classes\.(\w+)\.(\w+)"
    type_str = re.sub(pattern, r"\1.\2", type_str)

    # Also handle other cognite_toolkit paths by keeping only last two parts
    pattern = r"cognite_toolkit\.(?:[^.]+\.)*(\w+)\.(\w+)"
    type_str = re.sub(pattern, r"\1.\2", type_str)

    # Remove quotes that might be added by str()
    type_str = type_str.replace("'", "").replace('"', "")

    # Clean up class representation markers
    type_str = type_str.replace("<class ", "").replace(">", "")

    return type_str
