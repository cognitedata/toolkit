from typing import Any, Literal, TypeAlias

from pydantic import Field, JsonValue, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client._types import Metadata
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeUntypedId

# Property paths supported on document search filters; same paths are used for aggregate
# cardinalityValues / uniqueValues (and the special ["sourceFile", "metadata"] pair for
# cardinalityProperties / uniqueProperties). See
# https://api-docs.cognite.com/20230101/tag/Documents/operation/documentsSearch
DocumentPropertyPath: TypeAlias = (
    tuple[Literal["id"]]
    | tuple[Literal["externalId"]]
    | tuple[Literal["instanceId"]]
    | tuple[Literal["instanceId"], Literal["space"]]
    | tuple[Literal["instanceId"], Literal["externalId"]]
    | tuple[Literal["title"]]
    | tuple[Literal["author"]]
    | tuple[Literal["createdTime"]]
    | tuple[Literal["modifiedTime"]]
    | tuple[Literal["lastIndexedTime"]]
    | tuple[Literal["mimeType"]]
    | tuple[Literal["extension"]]
    | tuple[Literal["pageCount"]]
    | tuple[Literal["type"]]
    | tuple[Literal["geoLocation"]]
    | tuple[Literal["language"]]
    | tuple[Literal["assetIds"]]
    | tuple[Literal["assetExternalIds"]]
    | tuple[Literal["labels"]]
    | tuple[Literal["content"]]
    | tuple[Literal["sourceFile"], Literal["name"]]
    | tuple[Literal["sourceFile"], Literal["mimeType"]]
    | tuple[Literal["sourceFile"], Literal["size"]]
    | tuple[Literal["sourceFile"], Literal["source"]]
    | tuple[Literal["sourceFile"], Literal["directory"]]
    | tuple[Literal["sourceFile"], Literal["assetIds"]]
    | tuple[Literal["sourceFile"], Literal["assetExternalIds"]]
    | tuple[Literal["sourceFile"], Literal["datasetId"]]
    | tuple[Literal["sourceFile"], Literal["securityCategories"]]
    | tuple[Literal["sourceFile"], Literal["geoLocation"]]
    | tuple[Literal["sourceFile"], Literal["labels"]]
    | tuple[Literal["sourceFile"], Literal["metadata"]]
    | tuple[Literal["sourceFile"], Literal["metadata"], str]
)


class DocumentSourceFile(BaseModelObject):
    """Nested source file metadata on a document (Documents API)."""

    name: str
    directory: str | None = None
    source: str | None = None
    mime_type: str | None = None
    size: int | None = None
    content_hash: str | None = Field(default=None, alias="hash")
    asset_ids: list[int] | None = None
    labels: list[ExternalId] | None = None
    geo_location: JsonValue | None = None
    data_set_id: int | None = None
    security_categories: list[int] | None = None
    metadata: Metadata | None = None


class DocumentResponse(BaseModelObject):
    """One document from ``POST /documents/list`` (same shape as ``item`` in search hits)."""

    id: int
    external_id: str | None = None
    instance_id: NodeUntypedId | None = None
    title: str | None = None
    author: str | None = None
    producer: str | None = None
    created_time: int | None = None
    modified_time: int | None = None
    last_indexed_time: int | None = None
    mime_type: str | None = None
    extension: str | None = None
    page_count: int | None = None
    type: str | None = None
    language: str | None = None
    truncated_content: str | None = None
    asset_ids: list[int] | None = None
    labels: list[ExternalId] | None = None
    source_file: DocumentSourceFile | None = None
    geo_location: JsonValue | None = None


class DocumentSearchHighlight(BaseModelObject):
    """Highlight snippets for a document search hit (field name → HTML fragments)."""

    name: list[str] | None = None
    content: list[str] | None = None


class DocumentSearchHit(BaseModelObject):
    """One row from ``POST /documents/search``."""

    item: DocumentResponse
    highlight: DocumentSearchHighlight | None = None


class DocumentAggregateCountItem(BaseModelObject):
    """One row from a documents aggregate ``count`` or ``cardinality*`` response."""

    count: int


class DocumentUniqueBucket(BaseModelObject):
    """One bucket from a documents uniqueValues / uniqueProperties aggregate."""

    count: int
    values: list[str] | list[float] | list[ExternalId]

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        if "values" in out:
            v = out["values"]
            out["values"] = list(v) if isinstance(v, list) else [v]
        elif "value" in out:
            out["values"] = [out.pop("value")]
        else:
            out["values"] = []
        return out
