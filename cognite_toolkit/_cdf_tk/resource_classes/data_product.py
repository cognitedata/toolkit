from typing import Literal

from pydantic import Field

from .base import BaseModelResource, ToolkitResource


class SpaceReferenceYAML(BaseModelResource):
    space: str = Field(
        description="Space ID of the instance space.",
        min_length=1,
        max_length=43,
        pattern=r"^[a-zA-Z][a-zA-Z0-9_-]{0,41}[a-zA-Z0-9]?$",
    )


class OwnerGroupYAML(BaseModelResource):
    members: list[str] = Field(
        description="List of individual members with their details.",
    )


class DataModelReferenceYAML(BaseModelResource):
    space: str = Field(description="Space of the data model.")
    external_id: str = Field(description="External ID of the data model.")
    version: str = Field(description="Version of the data model.")


class InitialVersionYAML(BaseModelResource):
    data_model: DataModelReferenceYAML = Field(
        description="Details about the physical data model (non-materialized views).",
    )
    version_status: Literal["draft", "published", "deprecated", "archived"] | None = Field(
        default=None,
        description="Status of this data product version.",
    )
    usage_terms: str | None = Field(
        default=None,
        description="Permissible use and compliance terms.",
    )
    access: list[str] | None = Field(
        default=None,
        description="List of access groups or permissions.",
    )
    applications: dict[str, str] | None = Field(
        default=None,
        description="Associated applications as key-value pairs (e.g., Canvas, Charts, AI agents, Streamlits).",
    )
    tags: dict[str, str] | None = Field(
        default=None,
        description="Key-value pairs for categorization and filtering.",
    )


class DataProductYAML(ToolkitResource):
    external_id: str = Field(
        description="User-defined unique identifier for the Data Product.",
        min_length=1,
        max_length=100,
        pattern=r"^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    name: str = Field(
        description="Human-readable name of the Data Product.",
    )
    instance_read_spaces: list[SpaceReferenceYAML] = Field(
        description="List of instance spaces this data product reads data from.",
    )
    governance_status: Literal["governed", "ungoverned"] = Field(
        description="Governance status indicating whether the data product follows governance policies and standards.",
    )
    initial_version: InitialVersionYAML = Field(
        description="Initial version configuration for the data product. This will become the first data product version upon creation.",
    )
    description: str | None = Field(
        default=None,
        description="A detailed description of the Data Product.",
    )
    source_domains: list[str] | None = Field(
        default=None,
        description="List of Data Domains this data product is derived from.",
    )
    data_model_spaces: list[SpaceReferenceYAML] | None = Field(
        default=None,
        description="List of data model spaces associated with this data product.",
    )
    instance_write_space: SpaceReferenceYAML | None = Field(
        default=None,
        description="Optional instance space where this data product can write data.",
    )
    owner_group: OwnerGroupYAML | None = Field(
        default=None,
        description="Ownership information including group and member details for this data product.",
    )
