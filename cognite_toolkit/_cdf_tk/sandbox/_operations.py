"""Generic operations that mutate a ModelingSession."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.sandbox._session import ModelingSession


class Operation(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, extra="forbid")

    def apply(self, session: ModelingSession) -> None:
        raise NotImplementedError()


class _Target(Operation):
    """Points at a loaded resource by kind and identifier, as listed by the server."""

    kind: str
    identifier: dict[str, Any]
    folder: str | None = Field(default=None, description="Only needed if the kind exists in several folders.")


class SetPath(_Target):
    """Set the value at a path in the resource YAML. A ``-`` as the last step appends to a list."""

    path: list[str | int] = Field(min_length=1)
    value: Any

    def apply(self, session: ModelingSession) -> None:
        session.set_path(session.find(self.kind, self.identifier, self.folder), self.path, self.value)


class RemovePath(_Target):
    """Remove the key or list item at a path in the resource YAML."""

    path: list[str | int] = Field(min_length=1)

    def apply(self, session: ModelingSession) -> None:
        session.remove_path(session.find(self.kind, self.identifier, self.folder), self.path)


class SetExtra(_Target):
    """Replace the text file filling a resource field, for example the SQL ``query`` of a transformation.

    Keep ``{{ variable }}`` tokens in the text.
    """

    field: str
    text: str

    def apply(self, session: ModelingSession) -> None:
        session.set_extra(session.find(self.kind, self.identifier, self.folder), self.field, self.text)


class AddResource(Operation):
    """Add a resource in a new file, below the resource folder of the module."""

    kind: str
    resource: dict[str, Any]
    folder: str | None = Field(default=None, description="Only needed if the kind exists in several folders.")
    module: str | None = Field(default=None, description="Only needed if several modules are loaded.")
    sub_dir: str | None = Field(default=None, description="Defaults to the sub folder of the kind, if it has one.")

    def apply(self, session: ModelingSession) -> None:
        session.add_resource(self.kind, self.resource, self.folder, self.module, self.sub_dir)
