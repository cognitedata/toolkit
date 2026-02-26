from typing import Annotated, Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers.identifiers import DataProductVersionId, SemanticVersion
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource

SpaceId = Annotated[str, Field(pattern=SPACE_FORMAT_PATTERN, max_length=43)]


class ViewInstanceSpaces(BaseModelResource):
    read: list[SpaceId] = Field(
        default_factory=list, description="Instance space IDs from which you can read through this view."
    )
    write: list[SpaceId] = Field(
        default_factory=list, description="Instance space IDs to which you can write through this view."
    )


class DataProductVersionView(BaseModelResource):
    external_id: str = Field(description="External ID of the view in the data model.")
    instance_spaces: ViewInstanceSpaces = Field(
        default_factory=ViewInstanceSpaces, description="Instance spaces for this view."
    )


class DataProductVersionDataModel(BaseModelResource):
    external_id: str = Field(description="External ID of the referenced data model.")
    version: str = Field(description="Version of the referenced data model.")
    views: list[DataProductVersionView] = Field(
        default_factory=list,
        description="List of views with their instance spaces.",
        max_length=100,
    )


class DataProductVersionTerms(BaseModelResource):
    usage: str | None = Field(
        default=None, description="Permitted usage terms and conditions (markdown).", max_length=2000
    )
    limitations: str | None = Field(
        default=None, description="Usage limitations and restrictions (markdown).", max_length=500
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
    data_model: DataProductVersionDataModel = Field(
        description="Immutable reference to the data model version associated with this data product version.",
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

    def as_id(self) -> DataProductVersionId:
        return DataProductVersionId(
            data_product_external_id=self.data_product_external_id,
            version=self.version,
        )
