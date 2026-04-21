from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import DataProductVersionId, RuleSetVersionId, SemanticVersion
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource

SpaceId = Annotated[str, Field(pattern=SPACE_FORMAT_PATTERN, min_length=1, max_length=43)]


class ViewInstanceSpaces(BaseModelResource):
    read: list[SpaceId] = Field(
        default_factory=list, description="Instance space IDs from which you can read through this view."
    )
    write: list[SpaceId] = Field(
        default_factory=list, description="Instance space IDs to which you can write through this view."
    )


class DataProductVersionView(BaseModelResource):
    space: SpaceId = Field(description="The space where the view is located.")
    external_id: str = Field(description="External ID of the view.")
    version: str = Field(description="Version of the view.")
    instance_spaces: ViewInstanceSpaces = Field(
        default_factory=ViewInstanceSpaces, description="Instance spaces for this view."
    )


class DataProductVersionTerms(BaseModelResource):
    usage: str | None = Field(
        default=None, description="Permitted usage terms and conditions (markdown).", max_length=2000
    )
    limitations: str | None = Field(
        default=None, description="Usage limitations and restrictions (markdown).", max_length=500
    )


class DataProductVersionQualityRule(BaseModelResource):
    rule_set_external_id: str = Field(description="External ID of the referenced rule set.")
    version: SemanticVersion = Field(description="Version of the referenced rule set.")

    def as_id(self) -> RuleSetVersionId:
        return RuleSetVersionId(rule_set_external_id=self.rule_set_external_id, version=self.version)


class DataProductVersionQuality(BaseModelResource):
    rules: list[DataProductVersionQualityRule] = Field(
        default_factory=list,
        description="List of rule set version references applied to this data product version.",
        max_length=50,
    )


class DataProductVersionYAML(ToolkitResource):
    data_product_external_id: str = Field(
        description="External ID of the parent data product.",
        min_length=1,
        max_length=100,
        pattern=r"^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    version: SemanticVersion = Field(
        description="Semantic version of this data product version (major.minor.patch).",
    )
    views: list[DataProductVersionView] = Field(
        default_factory=list,
        description="Collection of view references (space, externalId, version, instanceSpaces) associated with this version.",
        max_length=100,
    )
    status: Literal["draft", "published", "deprecated"] = Field(
        default="draft",
        description="The status of this data product version.",
    )
    description: str | None = Field(
        default=None,
        description="A detailed description of this version (markdown).",
        max_length=2000,
    )
    terms: DataProductVersionTerms | None = Field(
        default=None,
        description="Terms and conditions for using this data product version.",
    )
    quality: DataProductVersionQuality | None = Field(
        default=None,
        description="Data quality rules applied to this version.",
    )

    def as_id(self) -> DataProductVersionId:
        return DataProductVersionId(
            data_product_external_id=self.data_product_external_id,
            version=self.version,
        )
