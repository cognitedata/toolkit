from collections.abc import Sequence
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, Field, model_serializer, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import Identifier


class InternalOrExternalIdDefinition(Identifier):
    type: str


class InternalIdUnwrapped(Identifier):
    id: int

    def __str__(self) -> str:
        return f"id={self.id}"

    @model_serializer
    def serialize(self) -> int:
        return self.id

    @model_validator(mode="before")
    @classmethod
    def deserialize(cls, value: Any) -> Any:
        if isinstance(value, int):
            return {"id": value}
        return value


class InternalId(InternalOrExternalIdDefinition):
    type: Literal["id"] = Field("id", exclude=True)
    id: int

    @classmethod
    def from_ids(cls, ids: Sequence[int]) -> list["InternalId"]:
        return [cls(id=id_) for id_ in ids]

    def __str__(self) -> str:
        return f"id={self.id}"

    def as_unwrapped(self) -> InternalIdUnwrapped:
        return InternalIdUnwrapped(id=self.id)


class ExternalId(InternalOrExternalIdDefinition):
    type: Literal["externalId"] = Field("externalId", exclude=True)
    external_id: str

    @classmethod
    def from_external_ids(cls, external_ids: list[str]) -> list["ExternalId"]:
        return [cls(external_id=ext_id) for ext_id in external_ids]

    def __str__(self) -> str:
        return f"externalId='{self.external_id}'"


InternalOrExternalId = Annotated[InternalId | ExternalId, Field(discriminator="type")]


class NameId(Identifier):
    name: str

    def __str__(self) -> str:
        return f"name='{self.name}'"


class RawDatabaseId(Identifier):
    name: str = Field(alias="name", validation_alias=AliasChoices("dbName", "name"))

    def __str__(self) -> str:
        return f"name='{self.name}'"

    def dump(self, camel_case: bool = True, exclude_extra: bool = False) -> dict[str, Any]:
        """Dump the resource to a dictionary.

        Args:
            camel_case (bool): Will be ignored. Included for compatibility.
            exclude_extra (bool): Will be ignored. Included for compatibility.

        """
        return self.model_dump(mode="json", by_alias=False)


class RawTableId(Identifier):
    name: str
    db_name: str

    def __str__(self) -> str:
        return f"dbName='{self.db_name}', name='{self.name}'"


class WorkflowVersionId(Identifier):
    workflow_external_id: str
    version: str

    def __str__(self) -> str:
        return f"workflowExternalId='{self.workflow_external_id}', version='{self.version}'"


class DataSetId(Identifier):
    data_set_id: int

    def __str__(self) -> str:
        return f"dataSetId={self.data_set_id}"
