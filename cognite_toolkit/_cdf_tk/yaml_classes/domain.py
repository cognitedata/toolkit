from pydantic import Field, field_validator

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.yaml_classes.base import ToolkitResource


class DomainYAML(ToolkitResource):
    external_id: str = Field(
        description="User-defined unique identifier for the domain.",
        min_length=1,
        max_length=100,
        pattern=r"^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    name: str = Field(
        description="Human-readable name of the domain.",
        min_length=1,
        max_length=50,
    )
    description: str | None = Field(
        default=None,
        description="A description of the domain.",
        max_length=4000,
    )

    @field_validator("external_id")
    @classmethod
    def validate_not_ungoverned(cls, value: str) -> str:
        if value.upper() == "UNGOVERNED":
            raise ValueError("'externalId' must not be 'UNGOVERNED', as it is reserved.")
        return value

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
