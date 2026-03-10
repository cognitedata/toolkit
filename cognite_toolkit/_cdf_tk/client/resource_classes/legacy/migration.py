import json
import warnings
from typing import Any, ClassVar

from pydantic import ConfigDict, Field, field_serializer, field_validator, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._wrapped import move_response_properties
from cognite_toolkit._cdf_tk.tk_warnings import IgnoredValueWarning
from cognite_toolkit._cdf_tk.utils.useful_types import AssetCentricType, AssetCentricTypeExtended

INSTANCE_SOURCE_VIEW_ID = ViewId(space="cognite_migration", external_id="InstanceSource", version="v1")
CREATED_SOURCE_SYSTEM_VIEW_ID = ViewId(space="cognite_migration", external_id="CreatedSourceSystem", version="v1")
SPACE_SOURCE_VIEW_ID = ViewId(space="cognite_migration", external_id="SpaceSource", version="v1")

_DMS_NODE_KEYS = frozenset(
    {
        "instanceType",
        "space",
        "externalId",
        "version",
        "createdTime",
        "lastUpdatedTime",
        "deletedTime",
    }
)


class AssetCentricId(BaseModelObject):
    model_config = ConfigDict(frozen=True)
    resource_type: AssetCentricTypeExtended
    id_: int = Field(alias="id")

    @property
    def id_value(self) -> int:
        """Generic name of the identifier.

        The AssetCentricExternalId has the same property. Thus, this means that these two
        classes can be used interchangeably when only the value of the identifier is needed, and not the type.
        """
        return self.id_

    def __str__(self) -> str:
        return f"{self.resource_type}(id={self.id_})"


def _dump_dms_node(model: BaseModelObject, view_id: ViewId, camel_case: bool = True) -> dict[str, Any]:
    """Dump a Pydantic model as a DMS node response with nested properties."""
    flat = model.model_dump(mode="json", by_alias=camel_case, exclude_unset=True)
    properties: dict[str, Any] = {}
    for key in list(flat):
        if key not in _DMS_NODE_KEYS:
            properties[key] = flat.pop(key)
    flat["properties"] = {
        view_id.space: {
            f"{view_id.external_id}/{view_id.version}": properties,
        }
    }
    return flat


class InstanceSource(BaseModelObject):
    """Pydantic model for reading InstanceSource nodes from the cognite_migration data model."""

    VIEW_ID: ClassVar[ViewId] = INSTANCE_SOURCE_VIEW_ID

    instance_type: str = "node"
    space: str
    external_id: str
    version: int
    created_time: int
    last_updated_time: int
    deleted_time: int | None = None

    resource_type: AssetCentricType
    id_: int = Field(alias="id")
    data_set_id: int | None = None
    classic_external_id: str | None = None
    preferred_consumer_view_id: ViewId | None = None
    ingestion_view: dict[str, str] | None = None

    @model_validator(mode="before")
    @classmethod
    def _extract_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        return move_response_properties(values, INSTANCE_SOURCE_VIEW_ID)

    @field_validator("preferred_consumer_view_id", mode="before")
    @classmethod
    def _validate_view_id(cls, v: Any) -> ViewId | None:
        if v is None:
            return None
        try:
            return ViewId.model_validate(v)
        except Exception as e:
            warnings.warn(
                IgnoredValueWarning(
                    name="InstanceSource.preferredConsumerViewId",
                    value=json.dumps(v) if not isinstance(v, str) else v,
                    reason=f"Invalid ViewId format expected 'space', 'externalId', 'version': {e!s}",
                )
            )
            return None

    @field_serializer("preferred_consumer_view_id")
    def _serialize_preferred_view(self, v: ViewId | None) -> dict[str, str] | None:
        if v is None:
            return None
        return v.dump()

    def dump(self, camel_case: bool = True, **kwargs: Any) -> dict[str, Any]:
        return _dump_dms_node(self, self.VIEW_ID, camel_case=camel_case)

    def as_asset_centric_id(self) -> AssetCentricId:
        return AssetCentricId(resource_type=self.resource_type, id_=self.id_)

    def consumer_view(self) -> ViewId:
        if self.preferred_consumer_view_id:
            return self.preferred_consumer_view_id
        if self.resource_type == "sequence":
            raise ValueError(f"Missing consumer view for sequence {self.external_id}.")
        external_id = {
            "asset": "CogniteAsset",
            "event": "CogniteActivity",
            "file": "CogniteFile",
            "timeseries": "CogniteTimeSeries",
        }[self.resource_type]
        return ViewId(space="cdf_cdm", external_id=external_id, version="v1")

    def as_node_id(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class CreatedSourceSystem(BaseModelObject):
    """Pydantic model for reading CreatedSourceSystem nodes from the cognite_migration data model."""

    VIEW_ID: ClassVar[ViewId] = CREATED_SOURCE_SYSTEM_VIEW_ID

    instance_type: str = "node"
    space: str
    external_id: str
    version: int
    created_time: int
    last_updated_time: int
    deleted_time: int | None = None

    source: str

    @model_validator(mode="before")
    @classmethod
    def _extract_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        return move_response_properties(values, CREATED_SOURCE_SYSTEM_VIEW_ID)

    def dump(self, camel_case: bool = True, **kwargs: Any) -> dict[str, Any]:
        return _dump_dms_node(self, self.VIEW_ID, camel_case=camel_case)

    def as_direct_relation_reference(self) -> NodeId:
        return NodeId(space=self.space, external_id=self.external_id)


class SpaceSource(BaseModelObject):
    """Pydantic model for reading SpaceSource nodes from the cognite_migration data model."""

    VIEW_ID: ClassVar[ViewId] = SPACE_SOURCE_VIEW_ID

    instance_type: str = "node"
    space: str
    external_id: str
    version: int
    created_time: int
    last_updated_time: int
    deleted_time: int | None = None

    instance_space: str
    data_set_id: int
    data_set_external_id: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _extract_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        return move_response_properties(values, SPACE_SOURCE_VIEW_ID)

    def dump(self, camel_case: bool = True, **kwargs: Any) -> dict[str, Any]:
        return _dump_dms_node(self, self.VIEW_ID, camel_case=camel_case)
