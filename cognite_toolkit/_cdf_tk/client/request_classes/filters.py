import sys
from typing import Any, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.resource_classes.annotation import AnnotationStatus, AnnotationType
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeReference, ViewReference
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId, InternalId

from .base import BaseModelRequest

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class Filter(BaseModelRequest): ...


class ClassicFilter(Filter):
    asset_subtree_ids: list[ExternalId | InternalId] | None = None
    data_set_ids: list[ExternalId | InternalId] | None = None

    @classmethod
    def from_asset_subtree_and_data_sets(
        cls,
        asset_subtree_id: str | int | list[str | int] | None = None,
        data_set_id: str | int | list[str | int] | None = None,
    ) -> Self:
        return cls(
            asset_subtree_ids=cls._as_internal_or_external_id_list(asset_subtree_id),
            data_set_ids=cls._as_internal_or_external_id_list(data_set_id),
        )

    @classmethod
    def _as_internal_or_external_id_list(
        cls, id: str | int | list[str | int] | None
    ) -> list[ExternalId | InternalId] | None:
        if id is None:
            return None
        ids = id if isinstance(id, list) else [id]
        return [ExternalId(external_id=item) if isinstance(item, str) else InternalId(id=item) for item in ids]


class DataModelingFilter(Filter):
    space: str | None = None
    include_global: bool | None = None


class ContainerFilter(DataModelingFilter):
    used_for: Literal["node", "edge", "record", "all"] | None


class ViewFilter(DataModelingFilter):
    include_inherited_properties: bool | None = None
    all_versions: bool | None = None


class DataModelFilter(DataModelingFilter):
    inline_views: bool | None = None
    all_versions: bool | None = None


class InstanceFilter(Filter):
    instance_type: Literal["node", "edge"] | None = None
    source: ViewReference | None = None
    space: list[str] | None = None

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        body: dict[str, Any] = {}
        if self.instance_type is not None:
            body["instanceType"] = self.instance_type
        if self.source is not None:
            body["sources"] = [{"source": self.source.dump()}]
        if self.space is not None:
            instance_type = self.instance_type or "node"
            body["filter"] = {
                "in": {
                    "property": [instance_type, "space"],
                    "values": self.space,
                }
            }
        return body


class AnnotationFilter(Filter):
    annotated_resource_type: Literal["file", "threedmodel"]
    annotated_resource_ids: list[ExternalId | InternalId]
    annotation_type: AnnotationType | None = None
    created_app: str | None = None
    creating_app_version: str | None = None
    creating_user: str | None = None
    status: AnnotationStatus | None = None


class ThreeDAssetMappingFilter(Filter): ...


class ThreeDAssetMappingAssetIdFilter(ThreeDAssetMappingFilter):
    asset_ids: list[int] = Field(max_length=100)


class ThreeDAssetMappingAssetInstanceIdFilter(ThreeDAssetMappingFilter):
    asset_instance_ids: list[NodeReference] = Field(max_length=100)


class ThreeDAssetMapping3DNodeFilter(ThreeDAssetMappingFilter):
    node_ids: list[int] = Field(max_length=100)


class ThreeDAssetMappingTreeIndexFilter(ThreeDAssetMappingFilter):
    tree_indexes: list[int] = Field(max_length=100)
