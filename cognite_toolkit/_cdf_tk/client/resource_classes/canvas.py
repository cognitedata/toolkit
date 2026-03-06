import sys
from collections import defaultdict
from collections.abc import Set
from datetime import datetime, timezone
from typing import Any, ClassVar, Literal
from uuid import uuid4

from pydantic import Field, JsonValue, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.identifiers import (
    EdgeId,
    InstanceDefinitionId,
    NodeId,
    ViewId,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import AssetCentricId

from .data_modeling import (
    EdgeRequest,
    WrappedInstanceListRequest,
    WrappedInstanceListResponse,
    move_response_properties,
)
from .data_modeling._wrapped import move_request_properties

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

CANVAS_INSTANCE_SPACE = "IndustrialCanvasInstanceSpace"
SOLUTION_TAG_SPACE = "SolutionTagsInstanceSpace"
CANVAS_SCHEMA_SPACE = "cdf_industrial_canvas"

CANVAS_VIEW_ID = ViewId(space=CANVAS_SCHEMA_SPACE, external_id="Canvas", version="v7")
CANVAS_ANNOTATION_VIEW_ID = ViewId(space=CANVAS_SCHEMA_SPACE, external_id="CanvasAnnotation", version="v1")
SOLUTION_TAG_VIEW_ID = ViewId(space="cdf_apps_shared", external_id="CogniteSolutionTag", version="v1")
CONTAINER_REFERENCE_VIEW_ID = ViewId(space=CANVAS_SCHEMA_SPACE, external_id="ContainerReference", version="v2")
FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID = ViewId(
    space=CANVAS_SCHEMA_SPACE, external_id="FdmInstanceContainerReference", version="v1"
)
ANNOTATION_EDGE_TYPE_REF = NodeId(space=CANVAS_SCHEMA_SPACE, external_id="referencesCanvasAnnotation")
CONTAINER_REFERENCE_EDGE_TYPE_REF = NodeId(space=CANVAS_SCHEMA_SPACE, external_id="referencesContainerReference")
FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF = NodeId(
    space=CANVAS_SCHEMA_SPACE, external_id="referencesFdmInstanceContainerReference"
)


class CanvasObject(BaseModelObject):
    VIEW_ID: ClassVar[ViewId]
    space: Literal["IndustrialCanvasInstanceSpace"] = CANVAS_INSTANCE_SPACE  # type: ignore[assignment]
    external_id: str
    id_: str = Field(alias="id")

    @model_validator(mode="before")
    @classmethod
    def move_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Move properties from sources to the top level."""
        return move_response_properties(values, cls.VIEW_ID)


class CanvasAnnotationItem(CanvasObject):
    """A canvas annotation node that is part of an IndustrialCanvas."""

    VIEW_ID: ClassVar[ViewId] = CANVAS_ANNOTATION_VIEW_ID
    annotation_type: str
    container_id: str | None = None
    is_selectable: bool | None = None
    is_draggable: bool | None = None
    is_resizable: bool | None = None
    properties_: dict[str, JsonValue] | None = Field(default=None, alias="properties")


class ContainerReferenceItem(CanvasObject):
    """A container reference node that is part of an IndustrialCanvas."""

    VIEW_ID: ClassVar[ViewId] = CONTAINER_REFERENCE_VIEW_ID
    container_reference_type: str
    resource_id: int
    resource_sub_id: int | None = None
    charts_id: str | None = None
    label: str | None = None
    properties_: dict[str, JsonValue] | None = Field(default=None, alias="properties")
    x: float | None = None
    y: float | None = None
    width: float | None = None
    height: float | None = None
    max_width: float | None = None
    max_height: float | None = None

    def as_asset_centric_id(self) -> AssetCentricId:
        if self.container_reference_type not in {"file", "timeseries", "asset", "event"}:
            raise ValueError(
                f"Container reference type '{self.container_reference_type}' is not supported for asset-centric ID."
            )
        return AssetCentricId(
            # Checked above
            resource_type=self.container_reference_type,  # type: ignore[arg-type]
            id_=self.resource_id,
        )


class FdmInstanceContainerReferenceItem(CanvasObject):
    """An FDM instance container reference node that is part of an IndustrialCanvas."""

    VIEW_ID: ClassVar[ViewId] = FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID
    container_reference_type: str
    instance_external_id: str
    instance_space: str
    view_external_id: str
    view_space: str
    view_version: str | None = None
    label: str | None = None
    properties_: dict[str, JsonValue] | None = Field(default=None, alias="properties")
    x: float | None = None
    y: float | None = None
    width: float | None = None
    height: float | None = None
    max_width: float | None = None
    max_height: float | None = None


class CogniteSolutionTagItem(BaseModelObject):
    """A Cognite solution tag node that is part of an IndustrialCanvas."""

    VIEW_ID: ClassVar[ViewId] = SOLUTION_TAG_VIEW_ID
    space: str = SOLUTION_TAG_SPACE
    external_id: str
    name: str
    description: str | None = None
    color: str | None = None

    @model_validator(mode="before")
    @classmethod
    def move_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Move properties from sources to the top level."""
        return move_response_properties(values, cls.VIEW_ID)


class CanvasProperties(BaseModelObject):
    """Properties of the Canvas node itself."""

    name: str
    created_by: str
    updated_at: datetime
    updated_by: str
    is_locked: bool | None = None
    visibility: str | None = None
    source_canvas_id: str | None = None
    is_archived: bool | None = None
    context: list[dict[str, JsonValue]] | None = None
    solution_tags: list[NodeId] | None = None

    annotations: list[CanvasAnnotationItem] | None = None
    container_references: list[ContainerReferenceItem] | None = None
    fdm_instance_container_references: list[FdmInstanceContainerReferenceItem] | None = None
    solution_tag_items: list[CogniteSolutionTagItem] | None = None


_CANVAS_EXCLUDE_FROM_PROPERTIES: Set[str] = frozenset(
    {
        "instance_type",
        "space",
        "external_id",
        "annotations",
        "container_references",
        "fdm_instance_container_references",
        "solution_tag_items",
    }
)

_SUB_ITEM_EXCLUDE: Set[str] = frozenset({"space", "external_id"})


def _dump_node(
    space: str,
    external_id: str,
    view_id: ViewId,
    properties: dict[str, Any],
) -> dict[str, Any]:
    return {
        "instanceType": "node",
        "space": space,
        "externalId": external_id,
        "sources": [{"source": view_id.dump(), "properties": properties}],
    }


class IndustrialCanvasRequest(WrappedInstanceListRequest, CanvasProperties):
    """Pydantic request model for an IndustrialCanvas."""

    VIEW_ID: ClassVar[ViewId] = CANVAS_VIEW_ID

    def dump(
        self, camel_case: bool = True, exclude_extra: bool = False, keep_existing_version: bool = True
    ) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        Args:
            camel_case (bool): Whether to use camelCase for the keys. Default is True.
            exclude_extra (bool): Whether to exclude extra fields not defined in the model. Default is False.

        """
        exclude: set[str] = set()
        if not keep_existing_version:
            exclude.add("existing_version")
        if exclude_extra:
            exclude |= set(self.__pydantic_extra__) if self.__pydantic_extra__ else set()
        return self.model_dump(mode="json", by_alias=camel_case, exclude_unset=True, exclude=exclude)

    def dump_instances(self) -> list[dict[str, Any]]:
        canvas_props = self.model_dump(
            mode="json",
            by_alias=True,
            exclude_unset=True,
            exclude=set(_CANVAS_EXCLUDE_FROM_PROPERTIES),
        )
        instances: list[dict[str, Any]] = [_dump_node(self.space, self.external_id, self.VIEW_ID, canvas_props)]

        edge_groups: list[
            tuple[
                list[CanvasAnnotationItem] | list[ContainerReferenceItem] | list[FdmInstanceContainerReferenceItem],
                ViewId,
                NodeId,
            ]
        ] = [
            (self.annotations or [], CANVAS_ANNOTATION_VIEW_ID, ANNOTATION_EDGE_TYPE_REF),
            (self.container_references or [], CONTAINER_REFERENCE_VIEW_ID, CONTAINER_REFERENCE_EDGE_TYPE_REF),
            (
                self.fdm_instance_container_references or [],
                FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID,
                FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF,
            ),
        ]
        for items, view_id, edge_type in edge_groups:
            for item in items:
                props = item.model_dump(mode="json", by_alias=True, exclude_unset=True, exclude=set(_SUB_ITEM_EXCLUDE))
                instances.append(_dump_node(item.space, item.external_id, view_id, props))
                instances.append(
                    EdgeRequest(
                        space=CANVAS_INSTANCE_SPACE,
                        external_id=f"{self.external_id}_{item.external_id}",
                        type=edge_type,
                        start_node=NodeId(space=CANVAS_INSTANCE_SPACE, external_id=self.external_id),
                        end_node=NodeId(space=CANVAS_INSTANCE_SPACE, external_id=item.external_id),
                    ).dump()
                )

        for tag in self.solution_tag_items or []:
            props = tag.model_dump(mode="json", by_alias=True, exclude_unset=True, exclude=set(_SUB_ITEM_EXCLUDE))
            instances.append(_dump_node(tag.space, tag.external_id, SOLUTION_TAG_VIEW_ID, props))

        return instances

    def as_ids(self) -> list[InstanceDefinitionId]:
        ids: list[InstanceDefinitionId] = [self.as_id()]

        edge_groups: list[
            list[CanvasAnnotationItem] | list[ContainerReferenceItem] | list[FdmInstanceContainerReferenceItem]
        ] = [self.annotations or [], self.container_references or [], self.fdm_instance_container_references or []]
        for items in edge_groups:
            for item in items:
                ids.append(NodeId(space=item.space, external_id=item.external_id))
                ids.append(
                    EdgeId(
                        space=CANVAS_INSTANCE_SPACE,
                        external_id=f"{self.external_id}_{item.external_id}",
                    )
                )
        return ids

    def create_backup(self) -> Self:
        """Create a backup copy of the IndustrialCanvasRequest instance with new IDs."""
        new_canvas_id = str(uuid4())

        new_canvas = self.model_copy(deep=True)
        new_canvas.external_id = new_canvas_id
        new_canvas.source_canvas_id = self.external_id
        new_canvas.updated_at = datetime.now(tz=timezone.utc)
        # Solution tags are not duplicated, they are reused
        generator: dict[str, str] = defaultdict(lambda: str(uuid4()))
        to_update: list[
            list[CanvasAnnotationItem] | list[ContainerReferenceItem] | list[FdmInstanceContainerReferenceItem]
        ] = []
        if self.annotations:
            to_update.append(self.annotations)
        if self.container_references:
            to_update.append(self.container_references)
        if self.fdm_instance_container_references:
            to_update.append(self.fdm_instance_container_references)
        for items in to_update:
            for item in items:
                item.id_ = generator[item.id_]
                item.external_id = f"{new_canvas_id}_{item.id_}"

        return new_canvas.replace_ids(generator)

    def replace_ids(self, id_mapping_old_by_new: dict[str, str]) -> Self:
        """Replace IDs in the IndustrialCanvasRequest instance based on the provided ID mapping.

        Args:
            id_mapping_old_by_new: A dictionary mapping old IDs to new IDs.
        Returns:
            A new IndustrialCanvasRequest instance with IDs replaced according to the provided mapping.
        """

        # There can be references to the old IDs in properties, for example, in annotations
        # the properties field there can be fromId and toId set.
        # We don't know all the places the Canvas application will have undocumented references,
        # so we do a recursive search and replace based on the id mapping we have created.
        dumped_data = self.dump(camel_case=True)

        def _replace_ids_recursively(data: Any, id_map: dict[str, str]) -> Any:
            if isinstance(data, dict):
                return {key: _replace_ids_recursively(value, id_map) for key, value in data.items()}
            if isinstance(data, list):
                return [_replace_ids_recursively(item, id_map) for item in data]
            if isinstance(data, str) and data in id_map:
                return id_map[data]
            return data

        updated_data = _replace_ids_recursively(dumped_data, id_mapping_old_by_new)

        return type(self)._load(updated_data)

    @model_validator(mode="before")
    @classmethod
    def move_properties(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Move properties from sources to the top level."""
        return move_request_properties(values)


class IndustrialCanvasResponse(WrappedInstanceListResponse, CanvasProperties):
    """Pydantic response model for an IndustrialCanvas."""

    VIEW_ID: ClassVar[ViewId] = CANVAS_VIEW_ID
    version: int = 0
    created_time: int = 0
    last_updated_time: int = 0
    deleted_time: int | None = None

    @classmethod
    def request_cls(cls) -> type[IndustrialCanvasRequest]:
        return IndustrialCanvasRequest

    def as_ids(self) -> list[InstanceDefinitionId]:
        ids: list[InstanceDefinitionId] = [self.as_id()]
        edge_groups: list[
            list[CanvasAnnotationItem] | list[ContainerReferenceItem] | list[FdmInstanceContainerReferenceItem]
        ] = [self.annotations or [], self.container_references or [], self.fdm_instance_container_references or []]
        for items in edge_groups:
            for item in items:
                ids.append(NodeId(space=item.space, external_id=item.external_id))
                ids.append(
                    EdgeId(
                        space=CANVAS_INSTANCE_SPACE,
                        external_id=f"{self.external_id}_{item.external_id}",
                    )
                )
        return ids
