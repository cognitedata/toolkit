from collections.abc import Set
from datetime import datetime
from typing import Any, ClassVar

from pydantic import JsonValue

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject

from .instance_api import (
    NodeReference,
    TypedEdgeIdentifier,
    TypedInstanceIdentifier,
    TypedNodeIdentifier,
    TypedViewReference,
    WrappedInstanceListRequest,
    WrappedInstanceListResponse,
)

CANVAS_INSTANCE_SPACE = "IndustrialCanvasInstanceSpace"
SOLUTION_TAG_SPACE = "SolutionTagsInstanceSpace"
CANVAS_SCHEMA_SPACE = "cdf_industrial_canvas"

CANVAS_VIEW_ID = TypedViewReference(space=CANVAS_SCHEMA_SPACE, external_id="Canvas", version="v7")
CANVAS_ANNOTATION_VIEW_ID = TypedViewReference(space=CANVAS_SCHEMA_SPACE, external_id="CanvasAnnotation", version="v1")
SOLUTION_TAG_VIEW_ID = TypedViewReference(space="cdf_apps_shared", external_id="CogniteSolutionTag", version="v1")
CONTAINER_REFERENCE_VIEW_ID = TypedViewReference(
    space=CANVAS_SCHEMA_SPACE, external_id="ContainerReference", version="v2"
)
FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID = TypedViewReference(
    space=CANVAS_SCHEMA_SPACE, external_id="FdmInstanceContainerReference", version="v1"
)

ANNOTATION_EDGE_TYPE_REF = {"space": CANVAS_SCHEMA_SPACE, "externalId": "referencesCanvasAnnotation"}
CONTAINER_REFERENCE_EDGE_TYPE_REF = {"space": CANVAS_SCHEMA_SPACE, "externalId": "referencesContainerReference"}
FDM_CONTAINER_REFERENCE_EDGE_TYPE_REF = {
    "space": CANVAS_SCHEMA_SPACE,
    "externalId": "referencesFdmInstanceContainerReference",
}


class CanvasAnnotationItem(BaseModelObject):
    """A canvas annotation node that is part of an IndustrialCanvas."""

    VIEW_ID: ClassVar[TypedViewReference] = CANVAS_ANNOTATION_VIEW_ID
    space: str = CANVAS_INSTANCE_SPACE
    external_id: str
    id_: str
    annotation_type: str
    container_id: str | None = None
    is_selectable: bool | None = None
    is_draggable: bool | None = None
    is_resizable: bool | None = None
    properties_: dict[str, JsonValue] | None = None


class ContainerReferenceItem(BaseModelObject):
    """A container reference node that is part of an IndustrialCanvas."""

    VIEW_ID: ClassVar[TypedViewReference] = CONTAINER_REFERENCE_VIEW_ID
    space: str = CANVAS_INSTANCE_SPACE
    external_id: str
    container_reference_type: str
    resource_id: int
    id_: str | None = None
    resource_sub_id: int | None = None
    charts_id: str | None = None
    label: str | None = None
    properties_: dict[str, JsonValue] | None = None
    x: float | None = None
    y: float | None = None
    width: float | None = None
    height: float | None = None
    max_width: float | None = None
    max_height: float | None = None


class FdmInstanceContainerReferenceItem(BaseModelObject):
    """An FDM instance container reference node that is part of an IndustrialCanvas."""

    VIEW_ID: ClassVar[TypedViewReference] = FDM_INSTANCE_CONTAINER_REFERENCE_VIEW_ID
    space: str = CANVAS_INSTANCE_SPACE
    external_id: str
    container_reference_type: str
    instance_external_id: str
    instance_space: str
    view_external_id: str
    view_space: str
    id_: str | None = None
    view_version: str | None = None
    label: str | None = None
    properties_: dict[str, JsonValue] | None = None
    x: float | None = None
    y: float | None = None
    width: float | None = None
    height: float | None = None
    max_width: float | None = None
    max_height: float | None = None


class CogniteSolutionTagItem(BaseModelObject):
    """A Cognite solution tag node that is part of an IndustrialCanvas."""

    VIEW_ID: ClassVar[TypedViewReference] = SOLUTION_TAG_VIEW_ID
    space: str = SOLUTION_TAG_SPACE
    external_id: str
    name: str
    description: str | None = None
    color: str | None = None


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
    solution_tags: list[NodeReference] | None = None


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
    view_id: TypedViewReference,
    properties: dict[str, Any],
) -> dict[str, Any]:
    return {
        "instanceType": "node",
        "space": space,
        "externalId": external_id,
        "sources": [{"source": view_id.dump(), "properties": properties}],
    }


def _dump_edge(
    space: str,
    external_id: str,
    edge_type: dict[str, str],
    start_space: str,
    start_external_id: str,
    end_space: str,
    end_external_id: str,
) -> dict[str, Any]:
    return {
        "instanceType": "edge",
        "space": space,
        "externalId": external_id,
        "type": edge_type,
        "startNode": {"space": start_space, "externalId": start_external_id},
        "endNode": {"space": end_space, "externalId": end_external_id},
    }


class IndustrialCanvasRequest(WrappedInstanceListRequest, CanvasProperties):
    """Pydantic request model for an IndustrialCanvas."""

    VIEW_ID: ClassVar[TypedViewReference] = CANVAS_VIEW_ID

    annotations: list[CanvasAnnotationItem] | None = None
    container_references: list[ContainerReferenceItem] | None = None
    fdm_instance_container_references: list[FdmInstanceContainerReferenceItem] | None = None
    solution_tag_items: list[CogniteSolutionTagItem] | None = None

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
                TypedViewReference,
                dict[str, str],
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
                    _dump_edge(
                        space=CANVAS_INSTANCE_SPACE,
                        external_id=f"{self.external_id}_{item.external_id}",
                        edge_type=edge_type,
                        start_space=self.space,
                        start_external_id=self.external_id,
                        end_space=item.space,
                        end_external_id=item.external_id,
                    )
                )

        for tag in self.solution_tag_items or []:
            props = tag.model_dump(mode="json", by_alias=True, exclude_unset=True, exclude=set(_SUB_ITEM_EXCLUDE))
            instances.append(_dump_node(tag.space, tag.external_id, SOLUTION_TAG_VIEW_ID, props))

        return instances

    def as_ids(self) -> list[TypedInstanceIdentifier]:
        ids: list[TypedInstanceIdentifier] = [self.as_id()]

        edge_groups: list[
            list[CanvasAnnotationItem] | list[ContainerReferenceItem] | list[FdmInstanceContainerReferenceItem]
        ] = [
            self.annotations or [],
            self.container_references or [],
            self.fdm_instance_container_references or [],
        ]
        for items in edge_groups:
            for item in items:
                ids.append(TypedNodeIdentifier(space=item.space, external_id=item.external_id))
                ids.append(
                    TypedEdgeIdentifier(
                        space=CANVAS_INSTANCE_SPACE,
                        external_id=f"{self.external_id}_{item.external_id}",
                    )
                )
        return ids


class IndustrialCanvasResponse(WrappedInstanceListResponse, CanvasProperties):
    """Pydantic response model for an IndustrialCanvas."""

    VIEW_ID: ClassVar[TypedViewReference] = CANVAS_VIEW_ID
    version: int = 0
    created_time: int = 0
    last_updated_time: int = 0
    deleted_time: int | None = None

    annotations: list[CanvasAnnotationItem] | None = None
    container_references: list[ContainerReferenceItem] | None = None
    fdm_instance_container_references: list[FdmInstanceContainerReferenceItem] | None = None
    solution_tag_items: list[CogniteSolutionTagItem] | None = None

    @classmethod
    def request_cls(cls) -> type[IndustrialCanvasRequest]:
        return IndustrialCanvasRequest

    def as_ids(self) -> list[TypedInstanceIdentifier]:
        ids: list[TypedInstanceIdentifier] = [TypedNodeIdentifier(space=self.space, external_id=self.external_id)]
        edge_groups: list[
            list[CanvasAnnotationItem] | list[ContainerReferenceItem] | list[FdmInstanceContainerReferenceItem]
        ] = [
            self.annotations or [],
            self.container_references or [],
            self.fdm_instance_container_references or [],
        ]
        for items in edge_groups:
            for item in items:
                ids.append(TypedNodeIdentifier(space=item.space, external_id=item.external_id))
                ids.append(
                    TypedEdgeIdentifier(
                        space=CANVAS_INSTANCE_SPACE,
                        external_id=f"{self.external_id}_{item.external_id}",
                    )
                )
        return ids
