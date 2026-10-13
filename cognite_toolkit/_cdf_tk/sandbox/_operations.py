"""Typed operations that mutate a ModelingSession."""

from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, DataModelId, ExternalId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._view_property import ViewCorePropertyRequest
from cognite_toolkit._cdf_tk.resource_ios import ContainerIO, ViewIO


class Operation(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid")

    def apply(self, session: Any) -> None:
        raise NotImplementedError()


class AddContainer(Operation):
    """Create a new container resource and source file."""

    container: ContainerRequest
    relative_dir: str | None = None

    def apply(self, session: Any) -> None:
        session.add_request(
            self.container,
            ContainerIO,
            sub_folder=ContainerIO.sub_folder_name,
            relative_dir=Path(self.relative_dir) if self.relative_dir else None,
        )


class AddContainerProperty(Operation):
    """Add a property to an existing container."""

    container: ContainerId
    name: str = Field(min_length=1)
    definition: ContainerPropertyDefinition

    def apply(self, session: Any) -> None:
        request = session.get_container(self.container)
        if self.name in request.properties:
            raise ValueError(f"Property '{self.name}' already exists on container {self.container}")
        request.properties[self.name] = self.definition
        session.mark_dirty(self.container)


class RemoveContainerProperty(Operation):
    """Remove a property from an existing container."""

    container: ContainerId
    name: str = Field(min_length=1)

    def apply(self, session: Any) -> None:
        request = session.get_container(self.container)
        if self.name not in request.properties:
            raise ValueError(f"Property '{self.name}' does not exist on container {self.container}")
        del request.properties[self.name]
        session.mark_dirty(self.container)


class AddView(Operation):
    """Create a new view resource and source file."""

    view: ViewRequest
    relative_dir: str | None = None

    def apply(self, session: Any) -> None:
        session.add_request(
            self.view,
            ViewIO,
            sub_folder=ViewIO.sub_folder_name,
            relative_dir=Path(self.relative_dir) if self.relative_dir else None,
        )


class AddViewProperty(Operation):
    """Add a core (container-mapped) property to an existing view."""

    view: ViewId
    name: str = Field(min_length=1)
    definition: ViewCorePropertyRequest

    def apply(self, session: Any) -> None:
        request = session.get_view(self.view)
        if request.properties is None:
            request.properties = {}
        if self.name in request.properties:
            raise ValueError(f"Property '{self.name}' already exists on view {self.view}")
        request.properties[self.name] = self.definition
        session.mark_dirty(self.view)


class RemoveViewProperty(Operation):
    """Remove a property from an existing view."""

    view: ViewId
    name: str = Field(min_length=1)

    def apply(self, session: Any) -> None:
        request = session.get_view(self.view)
        if not request.properties or self.name not in request.properties:
            raise ValueError(f"Property '{self.name}' does not exist on view {self.view}")
        del request.properties[self.name]
        session.mark_dirty(self.view)


class SetTransformationQuery(Operation):
    """Replace the SQL file text of a transformation. Keep ``{{ variable }}`` tokens in the query."""

    transformation: ExternalId
    query: str

    def apply(self, session: Any) -> None:
        session.set_query(self.transformation, self.query)


class AddViewToDataModel(Operation):
    """Append a view reference to a data model's views list."""

    data_model: DataModelId
    view: ViewId

    def apply(self, session: Any) -> None:
        if self.view not in session.requests:
            raise ValueError(f"View {self.view} is not loaded in the session")
        request = session.get_data_model(self.data_model)
        if request.views is None:
            request.views = []
        if self.view in request.views:
            raise ValueError(f"View {self.view} is already in data model {self.data_model}")
        request.views.append(self.view)
        session.mark_dirty(self.data_model)
