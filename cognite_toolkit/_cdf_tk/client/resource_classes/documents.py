from typing import Any, Literal, TypeAlias

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

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


class DocumentsApiItem(BaseModelObject):
    """One document or one aggregate bucket from Documents API list/aggregate responses."""


class DocumentUniqueBucket(BaseModelObject):
    """One bucket from a documents uniqueValues / uniqueProperties aggregate."""

    count: int
    values: list[Any]
