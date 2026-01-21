from collections.abc import Sequence
from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client._resource_base import Identifier


class InternalOrExternalIdDefinition(Identifier):
    type: str


class InternalId(InternalOrExternalIdDefinition):
    type: Literal["id"] = Field("id", exclude=True)
    id: int

    @classmethod
    def from_ids(cls, ids: Sequence[int]) -> list["InternalId"]:
        return [cls(id=id_) for id_ in ids]

    def __str__(self) -> str:
        return f"id={self.id}"


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


class WorkflowVersionId(Identifier):
    workflow_external_id: str
    version: str

    def __str__(self) -> str:
        return f"workflowExternalId='{self.workflow_external_id}', version='{self.version}'"


class DataSetId(Identifier):
    data_set_id: int

    def __str__(self) -> str:
        return f"dataSetId={self.data_set_id}"
