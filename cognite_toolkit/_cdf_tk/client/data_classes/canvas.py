from abc import ABC
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from cognite.client.data_classes.data_modeling import (
    DirectRelationReference,
    EdgeId,
    NodeId,
)
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    EdgeApply,
    InstanceApply,
    Node,
    NodeApply,
    NodeListWithCursor,
    PropertyOptions,
    T_Node,
    TypedNode,
    TypedNodeApply,
)

from cognite_toolkit._cdf_tk.client.data_classes.migration import AssetCentricId

CANVAS_INSTANCE_SPACE = "IndustrialCanvasInstanceSpace"
SOLUTION_TAG_SPACE = "SolutionTagsInstanceSpace"
CANVAS_SCHEMA_SPACE = "cdf_industrial_canvas"
ANNOTATION_EDGE_TYPE = DirectRelationReference(CANVAS_SCHEMA_SPACE, "referencesCanvasAnnotation")
CONTAINER_REFERENCE_EDGE_TYPE = DirectRelationReference(CANVAS_SCHEMA_SPACE, "referencesContainerReference")
FDM_CONTAINER_REFERENCE_EDGE_TYPE = DirectRelationReference(
    CANVAS_SCHEMA_SPACE, "referencesFdmInstanceContainerReference"
)


class ExtendedTypedNodeApply(TypedNodeApply, ABC):
    def dump(self, camel_case: bool = True, keep_existing_version: bool = True) -> dict[str, Any]:
        """Dumps the instance to a dictionary.

        Args:
            camel_case: If True, the keys will be in camelCase format.
            keep_existing_version: Whether to keep the existing version in the dumped data. Typically,
                you keep the existing version if you want to update the instance, and you remove it if
                you are creating a new instance (for example using the current node as a template).

        Returns:
            A dictionary representation of the instance.

        """
        output = super().dump(camel_case=camel_case)
        if not keep_existing_version:
            output.pop("existingVersion" if camel_case else "existing_version", None)
        return output


class _CanvasProperties:
    created_by = PropertyOptions("createdBy")
    updated_at = PropertyOptions("updatedAt")
    updated_by = PropertyOptions("updatedBy")
    is_locked = PropertyOptions("isLocked")
    source_canvas_id = PropertyOptions("sourceCanvasId")
    is_archived = PropertyOptions("isArchived")
    solution_tags = PropertyOptions("solutionTags")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_industrial_canvas", "Canvas", "v7")


class CanvasApply(_CanvasProperties, ExtendedTypedNodeApply):
    """This represents the writing format of canvas.

    It is used to when data is written to CDF.

    Args:
        external_id: The external id of the canva.
        name: The name or title of the canvas.
        created_by: The user identifier of the user that created the canvas.
        updated_at: The timestamp when the canvas was last updated.
        updated_by: The user identifier of the user that last updated the canvas.
        is_locked: The boolean state for handling canvas locking which is one-way operation from the user perspective
        visibility: The application-level visibility of the canvas. Must be either 'public' or 'private' and, if not
            set, the canvas is expected to be public.
        source_canvas_id: The property for handling versioning. Example sourceCanvasId === selectedCanvas -> query all
            versions of such canvas
        is_archived: Boolean that indicates whether the canvas is archived.
        context: Stores contextual data attached to the canvas, such as rules and pinned values.
        solution_tags: The list of solution tags associated with the canvas.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        external_id: str,
        *,
        name: str,
        created_by: str,
        updated_at: datetime,
        updated_by: str,
        is_locked: bool | None = None,
        visibility: str | None = None,
        source_canvas_id: str | None = None,
        is_archived: bool | None = None,
        context: list[dict] | None = None,
        solution_tags: list[DirectRelationReference | tuple[str, str]] | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, CANVAS_INSTANCE_SPACE, external_id, existing_version, type)
        self.name = name
        self.created_by = created_by
        self.updated_at = updated_at
        self.updated_by = updated_by
        self.is_locked = is_locked
        self.visibility = visibility
        self.source_canvas_id = source_canvas_id
        self.is_archived = is_archived
        self.context = context
        self.solution_tags = (
            [DirectRelationReference.load(solution_tag) for solution_tag in solution_tags] if solution_tags else None
        )


class Canvas(_CanvasProperties, TypedNode):
    """This represents the reading format of canva.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the canva.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        name: The name or title of the canvas.
        created_by: The user identifier of the user that created the canvas.
        updated_at: The timestamp when the canvas was last updated.
        updated_by: The user identifier of the user that last updated the canvas.
        is_locked: The boolean state for handling canvas locking which is one-way operation from the user perspective
        visibility: The application-level visibility of the canvas. Must be either 'public' or 'private' and, if not
            set, the canvas is expected to be public.
        source_canvas_id: The property for handling versioning. Example sourceCanvasId === selectedCanvas -> query all
            versions of such canvas
        is_archived: Boolean that indicates whether the canvas is archived.
        context: Stores contextual data attached to the canvas, such as rules and pinned values.
        solution_tags: The list of solution tags associated with the canvas.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str,
        created_by: str,
        updated_at: datetime,
        updated_by: str,
        is_locked: bool | None = None,
        visibility: str | None = None,
        source_canvas_id: str | None = None,
        is_archived: bool | None = None,
        context: list[dict] | None = None,
        solution_tags: list[DirectRelationReference] | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.created_by = created_by
        self.updated_at = updated_at
        self.updated_by = updated_by
        self.is_locked = is_locked
        self.visibility = visibility
        self.source_canvas_id = source_canvas_id
        self.is_archived = is_archived
        self.context = context
        self.solution_tags = (
            [DirectRelationReference.load(solution_tag) for solution_tag in solution_tags] if solution_tags else None
        )

    def as_write(self) -> CanvasApply:
        return CanvasApply(
            self.external_id,
            name=self.name,
            created_by=self.created_by,
            updated_at=self.updated_at,
            updated_by=self.updated_by,
            is_locked=self.is_locked,
            visibility=self.visibility,
            source_canvas_id=self.source_canvas_id,
            is_archived=self.is_archived,
            context=self.context,
            solution_tags=self.solution_tags,
            existing_version=self.version,
            type=self.type,
        )


class _CanvasAnnotationProperties:
    id_ = PropertyOptions("id")
    annotation_type = PropertyOptions("annotationType")
    container_id = PropertyOptions("containerId")
    is_selectable = PropertyOptions("isSelectable")
    is_draggable = PropertyOptions("isDraggable")
    is_resizable = PropertyOptions("isResizable")
    properties_ = PropertyOptions("properties")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_industrial_canvas", "CanvasAnnotation", "v1")


class CanvasAnnotationApply(_CanvasAnnotationProperties, ExtendedTypedNodeApply):
    """This represents the writing format of canvas annotation.

    It is used to when data is written to CDF.

    Args:
        external_id: The external id of the canvas annotation.
        id_: The unique identifier of the canvas annotation.
        annotation_type: The type of the annotation. Must be one of rectangle, ellipse, polyline, text or sticky.
        container_id: The optional ID of the container that the annotation is contained in.
        is_selectable: Boolean that indicates whether the annotation is selectable.
        is_draggable: Boolean that indicates whether the annotation is draggable.
        is_resizable: Boolean that indicates whether the annotation is resizable.
        properties_: Additional properties or configuration for the annotation.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        external_id: str,
        *,
        id_: str,
        annotation_type: str,
        container_id: str | None = None,
        is_selectable: bool | None = None,
        is_draggable: bool | None = None,
        is_resizable: bool | None = None,
        properties_: dict | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, CANVAS_INSTANCE_SPACE, external_id, existing_version, type)
        self.id_ = id_
        self.annotation_type = annotation_type
        self.container_id = container_id
        self.is_selectable = is_selectable
        self.is_draggable = is_draggable
        self.is_resizable = is_resizable
        self.properties_ = properties_


class CanvasAnnotation(_CanvasAnnotationProperties, TypedNode):
    """This represents the reading format of canvas annotation.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the canvas annotation.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        id_: The unique identifier of the canvas annotation.
        annotation_type: The type of the annotation. Must be one of rectangle, ellipse, polyline, text or sticky.
        container_id: The optional ID of the container that the annotation is contained in.
        is_selectable: Boolean that indicates whether the annotation is selectable.
        is_draggable: Boolean that indicates whether the annotation is draggable.
        is_resizable: Boolean that indicates whether the annotation is resizable.
        properties_: Additional properties or configuration for the annotation.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        id_: str,
        annotation_type: str,
        container_id: str | None = None,
        is_selectable: bool | None = None,
        is_draggable: bool | None = None,
        is_resizable: bool | None = None,
        properties_: dict | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.id_ = id_
        self.annotation_type = annotation_type
        self.container_id = container_id
        self.is_selectable = is_selectable
        self.is_draggable = is_draggable
        self.is_resizable = is_resizable
        self.properties_ = properties_

    def as_write(self) -> CanvasAnnotationApply:
        return CanvasAnnotationApply(
            self.external_id,
            id_=self.id_,
            annotation_type=self.annotation_type,
            container_id=self.container_id,
            is_selectable=self.is_selectable,
            is_draggable=self.is_draggable,
            is_resizable=self.is_resizable,
            properties_=self.properties_,
            existing_version=self.version,
            type=self.type,
        )


class _CogniteSolutionTagProperties:
    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_apps_shared", "CogniteSolutionTag", "v1")


class CogniteSolutionTagApply(_CogniteSolutionTagProperties, ExtendedTypedNodeApply):
    """This represents the writing format of Cognite solution tag.

    It is used to when data is written to CDF.

    Args:
        external_id: The external id of the Cognite solution tag.
        name: Name of the solution tag/label
        description: Description of the solution tag/label
        color: Color of the solution tag/label
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        external_id: str,
        *,
        name: str,
        description: str | None = None,
        color: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, SOLUTION_TAG_SPACE, external_id, existing_version, type)
        self.name = name
        self.description = description
        self.color = color


class CogniteSolutionTag(_CogniteSolutionTagProperties, TypedNode):
    """This represents the reading format of Cognite solution tag.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the Cognite solution tag.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        name: Name of the solution tag/label
        description: Description of the solution tag/label
        color: Color of the solution tag/label
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        name: str,
        description: str | None = None,
        color: str | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.name = name
        self.description = description
        self.color = color

    def as_write(self) -> CogniteSolutionTagApply:
        return CogniteSolutionTagApply(
            self.external_id,
            name=self.name,
            description=self.description,
            color=self.color,
            existing_version=self.version,
            type=self.type,
        )


class _ContainerReferenceProperties:
    container_reference_type = PropertyOptions("containerReferenceType")
    resource_id = PropertyOptions("resourceId")
    id_ = PropertyOptions("id")
    resource_sub_id = PropertyOptions("resourceSubId")
    charts_id = PropertyOptions("chartsId")
    properties_ = PropertyOptions("properties")
    max_width = PropertyOptions("maxWidth")
    max_height = PropertyOptions("maxHeight")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_industrial_canvas", "ContainerReference", "v2")


class ContainerReferenceApply(_ContainerReferenceProperties, ExtendedTypedNodeApply):
    """This represents the writing format of container reference.

    It is used to when data is written to CDF.

    Args:
        external_id: The external id of the container reference.
        container_reference_type: The type of the container. Must be one of file, timeseries, asset, event, threeD
        resource_id: The ID of the CDF resource associated with the container.
        id_: The unique identifier of the container reference.
        resource_sub_id: An optional sub ID of the CDF resource. Could, for example, be the revision id of a 3D model
        charts_id: An optional id for a charts' uuid. It is needed since all the other resources are using a numeric id
            - aka 'resourceId'.
        label: The label or name associated with the container.
        properties_: Additional properties or configuration for the container.
        x: The X-coordinate of the container in pixels.
        y: The Y-coordinate of the container in pixels.
        width: The width of the container in pixels.
        height: The height of the container in pixels.
        max_width: The maximum allowed width of the container in pixels.
        max_height: The maximum allowed height of the container in pixels.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        external_id: str,
        *,
        container_reference_type: str,
        resource_id: int,
        id_: str | None = None,
        resource_sub_id: int | None = None,
        charts_id: str | None = None,
        label: str | None = None,
        properties_: dict | None = None,
        x: float | None = None,
        y: float | None = None,
        width: float | None = None,
        height: float | None = None,
        max_width: float | None = None,
        max_height: float | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, CANVAS_INSTANCE_SPACE, external_id, existing_version, type)
        self.container_reference_type = container_reference_type
        self.resource_id = resource_id
        self.id_ = id_
        self.resource_sub_id = resource_sub_id
        self.charts_id = charts_id
        self.label = label
        self.properties_ = properties_
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.max_width = max_width
        self.max_height = max_height

    def as_asset_centric_id(self) -> AssetCentricId:
        """Returns the asset-centric ID of the container reference."""
        if self.container_reference_type not in {"file", "timeseries", "asset", "event"}:
            raise ValueError(
                f"Container reference type '{self.container_reference_type}' is not supported for asset-centric ID."
            )
        return AssetCentricId(
            self.container_reference_type,
            self.resource_id,
        )


class ContainerReference(_ContainerReferenceProperties, TypedNode):
    """This represents the reading format of container reference.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the container reference.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        container_reference_type: The type of the container. Must be one of file, timeseries, asset, event, threeD
        resource_id: The ID of the CDF resource associated with the container.
        id_: The unique identifier of the container reference.
        resource_sub_id: An optional sub ID of the CDF resource. Could, for example, be the revision id of a 3D model
        charts_id: An optional id for a charts' uuid. It is needed since all the other resources are using a numeric id
            - aka 'resourceId'.
        label: The label or name associated with the container.
        properties_: Additional properties or configuration for the container.
        x: The X-coordinate of the container in pixels.
        y: The Y-coordinate of the container in pixels.
        width: The width of the container in pixels.
        height: The height of the container in pixels.
        max_width: The maximum allowed width of the container in pixels.
        max_height: The maximum allowed height of the container in pixels.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        container_reference_type: str,
        resource_id: int,
        id_: str | None = None,
        resource_sub_id: int | None = None,
        charts_id: str | None = None,
        label: str | None = None,
        properties_: dict | None = None,
        x: float | None = None,
        y: float | None = None,
        width: float | None = None,
        height: float | None = None,
        max_width: float | None = None,
        max_height: float | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.container_reference_type = container_reference_type
        self.resource_id = resource_id
        self.id_ = id_
        self.resource_sub_id = resource_sub_id
        self.charts_id = charts_id
        self.label = label
        self.properties_ = properties_
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.max_width = max_width
        self.max_height = max_height

    def as_write(self) -> ContainerReferenceApply:
        return ContainerReferenceApply(
            self.external_id,
            container_reference_type=self.container_reference_type,
            resource_id=self.resource_id,
            id_=self.id_,
            resource_sub_id=self.resource_sub_id,
            charts_id=self.charts_id,
            label=self.label,
            properties_=self.properties_,
            x=self.x,
            y=self.y,
            width=self.width,
            height=self.height,
            max_width=self.max_width,
            max_height=self.max_height,
            existing_version=self.version,
            type=self.type,
        )

    def as_asset_centric_id(self) -> AssetCentricId:
        """Returns the asset-centric ID of the container reference."""
        if self.container_reference_type not in {"file", "timeseries", "asset", "event"}:
            raise ValueError(
                f"Container reference type '{self.container_reference_type}' is not supported for asset-centric ID."
            )
        return AssetCentricId(
            self.container_reference_type,
            self.resource_id,
        )


class _FdmInstanceContainerReferenceProperties:
    container_reference_type = PropertyOptions("containerReferenceType")
    instance_external_id = PropertyOptions("instanceExternalId")
    instance_space = PropertyOptions("instanceSpace")
    view_external_id = PropertyOptions("viewExternalId")
    view_space = PropertyOptions("viewSpace")
    id_ = PropertyOptions("id")
    view_version = PropertyOptions("viewVersion")
    properties_ = PropertyOptions("properties")
    max_width = PropertyOptions("maxWidth")
    max_height = PropertyOptions("maxHeight")

    @classmethod
    def get_source(cls) -> ViewId:
        return ViewId("cdf_industrial_canvas", "FdmInstanceContainerReference", "v1")


class FdmInstanceContainerReferenceApply(_FdmInstanceContainerReferenceProperties, ExtendedTypedNodeApply):
    """This represents the writing format of fdm instance container reference.

    It is used to when data is written to CDF.

    Args:
        external_id: The external id of the fdm instance container reference.
        container_reference_type: The container type of the FDM instance container. Must be 'fdmInstance'
        instance_external_id: The external ID of the instance
        instance_space: The space the instance belongs to
        view_external_id: The externalId of the view used to fetch the instance
        view_space: The space of the view used to fetch the instance
        id_: The application side ID of the FDM instance container reference.
        view_version: The version of the view used to fetch the instance. If not specified, the latest version is
            assumed to be used.
        label: The label or name associated with the container.
        properties_: Additional properties or configuration for the container.
        x: The X-coordinate of the container in pixels.
        y: The Y-coordinate of the container in pixels.
        width: The width of the container in pixels.
        height: The height of the container in pixels.
        max_width: The maximum allowed width of the container in pixels.
        max_height: The maximum allowed height of the container in pixels.
        existing_version: Fail the ingestion request if the node's version is greater than or equal to this value.
            If no existingVersion is specified, the ingestion will always overwrite any existing data for the node
            (for the specified container or node). If existingVersion is set to 0, the upsert will behave as an insert,
            so it will fail the bulk if the item already exists. If skipOnVersionConflict is set on the ingestion
            request, then the item will be skipped instead of failing the ingestion request.
        type: Direct relation pointing to the type node.
    """

    def __init__(
        self,
        external_id: str,
        *,
        container_reference_type: str,
        instance_external_id: str,
        instance_space: str,
        view_external_id: str,
        view_space: str,
        id_: str | None = None,
        view_version: str | None = None,
        label: str | None = None,
        properties_: dict | None = None,
        x: float | None = None,
        y: float | None = None,
        width: float | None = None,
        height: float | None = None,
        max_width: float | None = None,
        max_height: float | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, CANVAS_INSTANCE_SPACE, external_id, existing_version, type)
        self.container_reference_type = container_reference_type
        self.instance_external_id = instance_external_id
        self.instance_space = instance_space
        self.view_external_id = view_external_id
        self.view_space = view_space
        self.id_ = id_
        self.view_version = view_version
        self.label = label
        self.properties_ = properties_
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.max_width = max_width
        self.max_height = max_height


class FdmInstanceContainerReference(_FdmInstanceContainerReferenceProperties, TypedNode):
    """This represents the reading format of fdm instance container reference.

    It is used to when data is read from CDF.

    Args:
        space: The space where the node is located.
        external_id: The external id of the fdm instance container reference.
        version (int): DMS version.
        last_updated_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        created_time (int): The number of milliseconds since 00:00:00 Thursday, 1 January 1970,
            Coordinated Universal Time (UTC), minus leap seconds.
        container_reference_type: The container type of the FDM instance container. Must be 'fdmInstance'
        instance_external_id: The external ID of the instance
        instance_space: The space the instance belongs to
        view_external_id: The externalId of the view used to fetch the instance
        view_space: The space of the view used to fetch the instance
        id_: The application side ID of the FDM instance container reference.
        view_version: The version of the view used to fetch the instance. If not specified, the latest version is
            assumed to be used.
        label: The label or name associated with the container.
        properties_: Additional properties or configuration for the container.
        x: The X-coordinate of the container in pixels.
        y: The Y-coordinate of the container in pixels.
        width: The width of the container in pixels.
        height: The height of the container in pixels.
        max_width: The maximum allowed width of the container in pixels.
        max_height: The maximum allowed height of the container in pixels.
        type: Direct relation pointing to the type node.
        deleted_time: The number of milliseconds since 00:00:00 Thursday, 1 January 1970, Coordinated Universal Time
            (UTC), minus leap seconds. Timestamp when the instance was soft deleted. Note that deleted instances
            are filtered out of query results, but present in sync results
    """

    def __init__(
        self,
        space: str,
        external_id: str,
        version: int,
        last_updated_time: int,
        created_time: int,
        *,
        container_reference_type: str,
        instance_external_id: str,
        instance_space: str,
        view_external_id: str,
        view_space: str,
        id_: str | None = None,
        view_version: str | None = None,
        label: str | None = None,
        properties_: dict | None = None,
        x: float | None = None,
        y: float | None = None,
        width: float | None = None,
        height: float | None = None,
        max_width: float | None = None,
        max_height: float | None = None,
        type: DirectRelationReference | None = None,
        deleted_time: int | None = None,
    ) -> None:
        TypedNode.__init__(self, space, external_id, version, last_updated_time, created_time, deleted_time, type)
        self.container_reference_type = container_reference_type
        self.instance_external_id = instance_external_id
        self.instance_space = instance_space
        self.view_external_id = view_external_id
        self.view_space = view_space
        self.id_ = id_
        self.view_version = view_version
        self.label = label
        self.properties_ = properties_
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.max_width = max_width
        self.max_height = max_height

    def as_write(self) -> FdmInstanceContainerReferenceApply:
        return FdmInstanceContainerReferenceApply(
            self.external_id,
            container_reference_type=self.container_reference_type,
            instance_external_id=self.instance_external_id,
            instance_space=self.instance_space,
            view_external_id=self.view_external_id,
            view_space=self.view_space,
            id_=self.id_,
            view_version=self.view_version,
            label=self.label,
            properties_=self.properties_,
            x=self.x,
            y=self.y,
            width=self.width,
            height=self.height,
            max_width=self.max_width,
            max_height=self.max_height,
            existing_version=self.version,
            type=self.type,
        )


class IndustrialCanvasApply:
    """This class represents the writing format of IndustrialCanvas.
    It is used to when data is written to CDF.
    Args:
        canvas: The Canvas object.
        annotations: A list of CanvasAnnotation objects.
        container_references: A list of ContainerReference objects.
        fdm_instance_container_references: A list of FdmInstanceContainerReference objects.
        solution_tags: A list of CogniteSolutionTag objects.
    """

    def __init__(
        self,
        canvas: CanvasApply,
        annotations: list[CanvasAnnotationApply] | None = None,
        container_references: list[ContainerReferenceApply] | None = None,
        fdm_instance_container_references: list[FdmInstanceContainerReferenceApply] | None = None,
        solution_tags: list[CogniteSolutionTagApply] | None = None,
    ) -> None:
        self.canvas = canvas
        self.annotations: list[CanvasAnnotationApply] = annotations or []
        self.container_references: list[ContainerReferenceApply] = container_references or []
        self.fdm_instance_container_references: list[FdmInstanceContainerReferenceApply] = (
            fdm_instance_container_references or []
        )
        self.solution_tags: list[CogniteSolutionTagApply] = solution_tags or []

    def as_instances(self) -> list[InstanceApply]:
        """Convert the IndustrialCanvasApply to a list of InstanceApply objects."""
        instances: list[InstanceApply] = [self.canvas]
        instances.extend(self.annotations)
        instances.extend(self.container_references)
        instances.extend(self.fdm_instance_container_references)
        instances.extend(self.solution_tags)
        for items, edge_type in [
            (self.annotations, ANNOTATION_EDGE_TYPE),
            (self.container_references, CONTAINER_REFERENCE_EDGE_TYPE),
            (self.fdm_instance_container_references, FDM_CONTAINER_REFERENCE_EDGE_TYPE),
        ]:
            # MyPy does not recognize that items is a list of TypedNodeApply
            for item in items:  # type: ignore[attr-defined]
                instances.append(
                    EdgeApply(
                        space=CANVAS_INSTANCE_SPACE,
                        external_id=f"{self.canvas.external_id}_{item.external_id}",
                        start_node=DirectRelationReference(
                            space=self.canvas.space, external_id=self.canvas.external_id
                        ),
                        end_node=DirectRelationReference(space=item.space, external_id=item.external_id),
                        type=edge_type,
                    )
                )

        return instances

    def as_id(self) -> str:
        return self.canvas.external_id

    def as_instance_ids(self, include_solution_tags: bool = False) -> list[NodeId | EdgeId]:
        """Return a list of IDs for the instances in the IndustrialCanvasApply."""
        instances = self.as_instances()
        ids: list[NodeId | EdgeId] = []
        for instance in instances:
            if isinstance(instance, NodeApply):
                if include_solution_tags or not isinstance(instance, CogniteSolutionTagApply):
                    ids.append(NodeId(instance.space, instance.external_id))
            elif isinstance(instance, EdgeApply):
                ids.append(EdgeId(instance.space, instance.external_id))
            else:
                raise TypeError(f"Unexpected instance type: {type(instance)}")
        return ids

    def dump(self, keep_existing_version: bool = True) -> dict[str, object]:
        """Dump the IndustrialCanvasApply to a dictionary."""
        return {
            "canvas": self.canvas.dump(keep_existing_version=keep_existing_version),
            "annotations": [
                annotation.dump(keep_existing_version=keep_existing_version) for annotation in self.annotations
            ],
            "containerReferences": [
                container_ref.dump(keep_existing_version=keep_existing_version)
                for container_ref in self.container_references
            ],
            "fdmInstanceContainerReferences": [
                fdm_instance_container_ref.dump(keep_existing_version=keep_existing_version)
                for fdm_instance_container_ref in self.fdm_instance_container_references
            ],
            "solutionTags": [
                solution_tag.dump(keep_existing_version=keep_existing_version) for solution_tag in self.solution_tags
            ],
        }

    def create_backup(self) -> "IndustrialCanvasApply":
        """Create a duplicate of the IndustrialCanvasApply instance."""
        new_canvas_id = str(uuid4())
        new_canvas = CanvasApply._load(self.canvas.dump(keep_existing_version=False))
        new_canvas.external_id = new_canvas_id
        new_canvas.source_canvas_id = self.canvas.external_id
        new_canvas.updated_at = datetime.now(tz=timezone.utc)
        # Solution tags are not duplicated, they are reused
        new_container = IndustrialCanvasApply(new_canvas, [], [], [], solution_tags=self.solution_tags)
        items: list[ContainerReferenceApply] | list[CanvasAnnotationApply] | list[FdmInstanceContainerReferenceApply]
        item_cls: type[CanvasAnnotationApply] | type[ContainerReferenceApply] | type[FdmInstanceContainerReferenceApply]
        new_item_list: list[NodeApply]
        for items, item_cls, new_item_list in [  # type: ignore[assignment]
            (self.annotations, CanvasAnnotationApply, new_container.annotations),
            (self.container_references, ContainerReferenceApply, new_container.container_references),
            (
                self.fdm_instance_container_references,
                FdmInstanceContainerReferenceApply,
                new_container.fdm_instance_container_references,
            ),
        ]:
            for item in items:
                # Serialize the item to create a new instance
                new_item = item_cls._load(item.dump(keep_existing_version=False))
                new_item.id_ = str(uuid4())
                new_item.external_id = f"{new_canvas_id}_{new_item.external_id}"
                new_item_list.append(new_item)
        return new_container


class IndustrialCanvas:
    """This class represents one instance of the Canvas with all connected data."""

    def __init__(
        self,
        canvas: Canvas,
        annotations: NodeListWithCursor[CanvasAnnotation] | None = None,
        container_references: NodeListWithCursor[ContainerReference] | None = None,
        fdm_instance_container_references: NodeListWithCursor[FdmInstanceContainerReference] | None = None,
        solution_tags: NodeListWithCursor[CogniteSolutionTag] | None = None,
    ) -> None:
        self.canvas = canvas
        self.annotations = annotations or NodeListWithCursor[CanvasAnnotation]([], None)
        self.container_references = container_references or NodeListWithCursor[ContainerReference]([], None)
        self.fdm_instance_container_references = fdm_instance_container_references or NodeListWithCursor[
            FdmInstanceContainerReference
        ]([], None)
        self.solution_tags = solution_tags or NodeListWithCursor[CogniteSolutionTag]([], None)

    @classmethod
    def load(cls, resource: Mapping[str, list]) -> "IndustrialCanvas":
        """Load an IndustrialCanvas instance from a QueryResult."""
        if not ("canvas" in resource and isinstance(resource["canvas"], Sequence) and len(resource["canvas"]) == 1):
            raise ValueError("Resource does not contain a canvas node.")
        canvas_resource = resource["canvas"][0]
        if isinstance(canvas_resource, dict):
            canvas = Canvas._load(canvas_resource)
        elif isinstance(canvas_resource, Canvas):
            canvas = canvas_resource
        elif isinstance(canvas_resource, Node):
            canvas = Canvas._load(canvas_resource.dump())
        else:
            raise TypeError(f"Canvas resource {type(canvas_resource)} is not supported.")
        return cls(
            canvas=canvas,
            annotations=cls._load_items(resource.get("annotations"), CanvasAnnotation),
            container_references=cls._load_items(resource.get("containerReferences"), ContainerReference),
            fdm_instance_container_references=cls._load_items(
                resource.get("fdmInstanceContainerReferences"), FdmInstanceContainerReference
            ),
            solution_tags=cls._load_items(resource.get("solutionTags"), CogniteSolutionTag),
        )

    @classmethod
    def _load_items(cls, items: object | None, node_cls: type[T_Node]) -> NodeListWithCursor[T_Node]:
        if items is None:
            return NodeListWithCursor[T_Node]([], None)
        elif isinstance(items, Sequence):
            nodes: list[T_Node] = []
            for node in items:
                if isinstance(node, dict):
                    # Bug in PySDK node_cls._load returns an instances of T_Node
                    nodes.append(node_cls._load(node))  # type: ignore[arg-type]
                elif isinstance(node, node_cls):
                    nodes.append(node)
                elif isinstance(node, Node):
                    # Bug in PySDK, node_cls._load returns an instance of T_Node
                    nodes.append(node_cls._load(node.dump()))  # type: ignore[arg-type]
                else:
                    raise TypeError(f"Expected a sequence of {node_cls.__name__}, got {type(node).__name__}")
            return NodeListWithCursor[T_Node](
                nodes,
                items.cursor if isinstance(items, NodeListWithCursor) else None,
            )
        raise TypeError(f"Expected a sequence of {node_cls.__name__}, got {type(items).__name__}")

    def dump(self) -> dict[str, list]:
        """Dump the IndustrialCanvas to a dictionary."""
        return {
            "canvas": [self.canvas.dump()],
            "annotations": [annotation.dump() for annotation in self.annotations],
            "containerReferences": [container_ref.dump() for container_ref in self.container_references],
            "fdmInstanceContainerReferences": [
                fdm_instance_container_ref.dump()
                for fdm_instance_container_ref in self.fdm_instance_container_references
            ],
            "solutionTags": [solution_tag.dump() for solution_tag in self.solution_tags],
        }

    def as_write(self) -> "IndustrialCanvasApply":
        return IndustrialCanvasApply(
            canvas=self.canvas.as_write(),
            annotations=[annotation.as_write() for annotation in self.annotations],
            container_references=[container_ref.as_write() for container_ref in self.container_references],
            fdm_instance_container_references=[
                fdm_instance_container_ref.as_write()
                for fdm_instance_container_ref in self.fdm_instance_container_references
            ],
            solution_tags=[solution_tag.as_write() for solution_tag in self.solution_tags],
        )
