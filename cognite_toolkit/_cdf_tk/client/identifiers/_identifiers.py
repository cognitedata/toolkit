from collections.abc import Iterable, Sequence
from typing import Annotated, Any, Literal

from pydantic import AliasChoices, Field, model_serializer, model_validator

from cognite_toolkit._cdf_tk.client._resource_base import Identifier

SemanticVersion = Annotated[
    str,
    Field(
        min_length=5,
        max_length=14,
        pattern=r"^(0|[1-9]\d{0,3})\.(0|[1-9]\d{0,3})\.(0|[1-9]\d{0,3})$",
    ),
]


class InternalOrExternalIdDefinitionId(Identifier):
    type: str


class InternalUnwrappedId(Identifier):
    id: int

    def __str__(self) -> str:
        return f"id={self.id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"id-{self.id}"
        return str(self.id)

    @model_serializer
    def serialize(self) -> int:
        return self.id

    @model_validator(mode="before")
    @classmethod
    def deserialize(cls, value: Any) -> Any:
        if isinstance(value, int):
            return {"id": value}
        return value


class InternalId(InternalOrExternalIdDefinitionId):
    type: Literal["id"] = Field("id", exclude=True)
    id: int

    @classmethod
    def from_ids(cls, ids: Sequence[int]) -> list["InternalId"]:
        return [cls(id=id_) for id_ in ids]

    def __str__(self) -> str:
        return f"id={self.id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"id-{self.id}"
        return str(self.id)

    def as_unwrapped(self) -> InternalUnwrappedId:
        return InternalUnwrappedId(id=self.id)


class ExternalId(InternalOrExternalIdDefinitionId):
    type: Literal["externalId"] = Field("externalId", exclude=True)
    external_id: str

    @classmethod
    def from_external_ids(cls, external_ids: Iterable[str]) -> list["ExternalId"]:
        return [cls(external_id=ext_id) for ext_id in external_ids]

    def __str__(self) -> str:
        return f"externalId='{self.external_id}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"externalId-{self.external_id}"
        return self.external_id


InternalOrExternalId = Annotated[InternalId | ExternalId, Field(discriminator="type")]


class NameId(Identifier):
    name: str

    def __str__(self) -> str:
        return f"name='{self.name}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"name-{self.name}"
        return self.name


class RawDatabaseId(Identifier):
    name: str = Field(alias="name", validation_alias=AliasChoices("dbName", "name"))

    def __str__(self) -> str:
        return f"name='{self.name}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"name-{self.name}"
        return self.name

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
        return f"{self.db_name}.{self.name}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"dbName-{self.db_name}.name-{self.name}"
        return f"{self.db_name}.{self.name}"


class SequenceRowId(Identifier):
    external_id: str = Field(description="ExternalId of the sequence")
    rows: tuple[int, ...]

    def __str__(self) -> str:
        rows_str = ", ".join(str(row) for row in self.rows)
        return f"externalId='{self.external_id}', rows=[{rows_str}]"

    def _as_filename(self, include_type: bool = False) -> str:
        rows_str = "-".join(str(row) for row in self.rows)
        if include_type:
            return f"externalId-{self.external_id}.rows-{rows_str}"
        return f"{self.external_id}.{rows_str}"


class ExtractionPipelineConfigId(Identifier):
    external_id: str
    revision: int | None = None

    def __str__(self) -> str:
        return f"externalId='{self.external_id}', revision={self.revision}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"externalId-{self.external_id}.revision-{self.revision}"
        return f"{self.external_id}.{self.revision}"


class WorkflowVersionId(Identifier):
    workflow_external_id: str
    version: str

    def __str__(self) -> str:
        return f"workflowExternalId='{self.workflow_external_id}', version='{self.version}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"workflowExternalId-{self.workflow_external_id}.version-{self.version}"
        return f"{self.workflow_external_id}.{self.version}"


class ThreeDModelRevisionId(Identifier):
    model_id: int = Field(exclude=True)
    id: int

    def __str__(self) -> str:
        return f"modelId={self.model_id}, id={self.id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"modelId-{self.model_id}.id-{self.id}"
        return f"{self.model_id}.{self.id}"


class DataSetId(Identifier):
    data_set_id: int

    def __str__(self) -> str:
        return f"dataSetId={self.data_set_id}"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"dataSetId-{self.data_set_id}"
        return str(self.data_set_id)


class DataProductVersionId(Identifier):
    data_product_external_id: str
    version: SemanticVersion

    def __str__(self) -> str:
        return f"dataProductExternalId='{self.data_product_external_id}', version='{self.version}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"dataProductExternalId-{self.data_product_external_id}.version-{self.version}"
        return f"{self.data_product_external_id}.{self.version}"


class TransformationNotificationId(Identifier):
    transformation_external_id: str
    destination: str

    def __str__(self) -> str:
        return f"transformationExternalId='{self.transformation_external_id}', destination='{self.destination}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"transformationExternalId-{self.transformation_external_id}.destination-{self.destination}"
        return f"{self.transformation_external_id}.{self.destination}"


class PrincipalId(Identifier):
    id: str

    def __str__(self) -> str:
        return f"id='{self.id}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"id-{self.id}"
        return self.id


class UserProfileId(Identifier):
    user_identifier: str

    def __str__(self) -> str:
        return f"userIdentifier='{self.user_identifier}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"userIdentifier-{self.user_identifier}"
        return self.user_identifier


class PrincipalLoginId(Identifier):
    principal: str
    id: str

    def __str__(self) -> str:
        return f"principal='{self.principal}', id='{self.id}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"principal-{self.principal}.id-{self.id}"
        return f"{self.principal}.{self.id}"


class RuleSetVersionId(Identifier):
    rule_set_external_id: str
    version: SemanticVersion

    def __str__(self) -> str:
        return f"ruleSetExternalId='{self.rule_set_external_id}', version='{self.version}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"ruleSetExternalId-{self.rule_set_external_id}.version-{self.version}"
        return f"{self.rule_set_external_id}.{self.version}"


class SignalSinkId(Identifier):
    type: Literal["email", "user"]
    external_id: str

    def __str__(self) -> str:
        return f"type='{self.type}', externalId='{self.external_id}'"

    def _as_filename(self, include_type: bool = False) -> str:
        if include_type:
            return f"type-{self.type}.externalId-{self.external_id}"
        return f"{self.type}.{self.external_id}"
