from datetime import datetime

from cognite.client.data_classes.data_modeling import DirectRelationReference
from cognite.client.data_classes.data_modeling.ids import ViewId
from cognite.client.data_classes.data_modeling.instances import (
    PropertyOptions,
    TypedNode,
    TypedNodeApply,
)


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


class CanvasApply(_CanvasProperties, TypedNodeApply):
    """This represents the writing format of canvas.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
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
        space: str,
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
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
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
            self.space,
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
            solution_tags=self.solution_tags,  # type: ignore[arg-type]
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


class CanvasAnnotationApply(_CanvasAnnotationProperties, TypedNodeApply):
    """This represents the writing format of canvas annotation.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
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
        space: str,
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
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
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
            self.space,
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


class CogniteSolutionTagApply(_CogniteSolutionTagProperties, TypedNodeApply):
    """This represents the writing format of Cognite solution tag.

    It is used to when data is written to CDF.

    Args:
        space: The space where the node is located.
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
        space: str,
        external_id: str,
        *,
        name: str,
        description: str | None = None,
        color: str | None = None,
        existing_version: int | None = None,
        type: DirectRelationReference | tuple[str, str] | None = None,
    ) -> None:
        TypedNodeApply.__init__(self, space, external_id, existing_version, type)
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
            self.space,
            self.external_id,
            name=self.name,
            description=self.description,
            color=self.color,
            existing_version=self.version,
            type=self.type,
        )
