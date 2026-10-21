from typing import NamedTuple

from pydantic import JsonValue


class ViewReference(NamedTuple):
    space: str
    external_id: str
    version: str


def find_view_references(query: JsonValue) -> list[ViewReference] | None:
    """Finds the views a data modeling query reads from.

    The query is untyped JSON, so this collects every view reference ({"type": "view", "space": ..., ...}),
    wherever it is in the query (hasData filters, select sources, through). Returns None when the query is not
    an object or contains no view reference, as the shape is then unknown.
    """
    found: dict[ViewReference, None] = {}
    _collect(query, found)
    if not isinstance(query, dict) or not found:
        return None
    return list(found)


def _collect(value: JsonValue, found: dict[ViewReference, None]) -> None:
    if isinstance(value, dict):
        space, external_id, version = value.get("space"), value.get("externalId"), value.get("version")
        if value.get("type") == "view" and isinstance(space, str) and isinstance(external_id, str):
            if isinstance(version, str):
                found[ViewReference(space, external_id, version)] = None
        for item in value.values():
            _collect(item, found)
    elif isinstance(value, list):
        for item in value:
            _collect(item, found)
