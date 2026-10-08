from typing import Any, Literal, TypeAlias

from pydantic import JsonValue, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

AssetPropertyPath: TypeAlias = (
    tuple[Literal["labels"]]
    | tuple[Literal["createdTime"]]
    | tuple[Literal["dataSetId"]]
    | tuple[Literal["id"]]
    | tuple[Literal["lastUpdatedTime"]]
    | tuple[Literal["parentId"]]
    | tuple[Literal["rootId"]]
    | tuple[Literal["description"]]
    | tuple[Literal["externalId"]]
    | tuple[Literal["metadata"]]
    | tuple[Literal["metadata"], str]
    | tuple[Literal["name"]]
    | tuple[Literal["source"]]
)

EventPropertyPath: TypeAlias = (
    tuple[Literal["assetIds"]]
    | tuple[Literal["createdTime"]]
    | tuple[Literal["dataSetId"]]
    | tuple[Literal["endTime"]]
    | tuple[Literal["id"]]
    | tuple[Literal["lastUpdatedTime"]]
    | tuple[Literal["startTime"]]
    | tuple[Literal["description"]]
    | tuple[Literal["externalId"]]
    | tuple[Literal["metadata"]]
    | tuple[Literal["metadata"], str]
    | tuple[Literal["source"]]
    | tuple[Literal["subtype"]]
    | tuple[Literal["type"]]
)

TimeSeriesPropertyPath: TypeAlias = (
    tuple[Literal["description"]]
    | tuple[Literal["externalId"]]
    | tuple[Literal["name"]]
    | tuple[Literal["unit"]]
    | tuple[Literal["unitExternalId"]]
    | tuple[Literal["unitQuantity"]]
    | tuple[Literal["assetId"]]
    | tuple[Literal["assetRootId"]]
    | tuple[Literal["createdTime"]]
    | tuple[Literal["dataSetId"]]
    | tuple[Literal["id"]]
    | tuple[Literal["lastUpdatedTime"]]
    | tuple[Literal["isStep"]]
    | tuple[Literal["isString"]]
    | tuple[Literal["accessCategories"]]
    | tuple[Literal["securityCategories"]]
    | tuple[Literal["metadata"]]
    | tuple[Literal["metadata"], str]
)

SequencePropertyPath: TypeAlias = (
    tuple[Literal["description"]]
    | tuple[Literal["externalId"]]
    | tuple[Literal["name"]]
    | tuple[Literal["assetId"]]
    | tuple[Literal["assetRootId"]]
    | tuple[Literal["createdTime"]]
    | tuple[Literal["dataSetId"]]
    | tuple[Literal["id"]]
    | tuple[Literal["lastUpdatedTime"]]
    | tuple[Literal["accessCategories"]]
    | tuple[Literal["metadata"]]
    | tuple[Literal["metadata"], str]
)


class ClassicAggregateCountItem(BaseModelObject):
    """One row from a classic ``count`` or ``cardinality*`` aggregate."""

    count: int


class ClassicAggregateUniqueBucket(BaseModelObject):
    """One bucket from a classic ``uniqueValues`` or ``uniqueProperties`` aggregate."""

    count: int
    values: list[JsonValue]

    @property
    def value(self) -> JsonValue:
        if not self.values:
            raise ValueError("No values in this bucket")
        return self.values[0]

    @model_validator(mode="before")
    @classmethod
    def _normalize_values(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        out = dict(data)
        if "values" in out:
            values = out["values"]
            out["values"] = list(values) if isinstance(values, list) else [values]
        elif "value" in out:
            out["values"] = [out.pop("value")]
        else:
            out["values"] = []
        return out
