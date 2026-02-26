from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import ToolkitResource


class DataProductYAML(ToolkitResource):
    external_id: str = Field(
        description="User-defined unique identifier for the Data Product.",
        min_length=1,
        max_length=100,
        pattern=r"^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    name: str = Field(
        description="Human-readable name of the Data Product.",
        min_length=1,
        max_length=50,
    )
    schema_space: str | None = Field(
        default=None,
        description="The schema space where the data product's data model and views are located. Defaults to the data product's externalId.",
    )
    description: str | None = Field(
        default=None,
        description="A description of the Data Product.",
        max_length=200,
    )
    is_governed: bool = Field(
        default=False,
        description="Indicates whether the data product follows governance policies and standards.",
    )
    tags: list[str] | None = Field(
        default=None, description="A list of distinct tags for categorization and filtering.", max_length=10
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
