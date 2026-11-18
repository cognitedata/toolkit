from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, SPACE_FORMAT_PATTERN

from .base import BaseModelResource, ToolkitResource


class InstanceScopeView(BaseModelResource):
    type: Literal["view"] = Field(default="view", description="The kind of resource this scope applies to.")
    space: str = Field(
        description="The space where the view is located.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(description="External ID of the view.", pattern=DM_EXTERNAL_ID_PATTERN)


class DataProductInstanceScope(BaseModelResource):
    source: InstanceScopeView = Field(description="The view this scope applies to. It covers all versions of the view.")
    read: list[Annotated[str, Field(pattern=SPACE_FORMAT_PATTERN)]] = Field(
        description="Instance spaces that can be read through this view when using the data product.",
        min_length=1,
        max_length=100,
    )
    upsert: list[Annotated[str, Field(pattern=SPACE_FORMAT_PATTERN)]] = Field(
        default_factory=list,
        description="Instance spaces that can be upserted through this view when using the data product.",
        max_length=100,
    )
    delete: list[Annotated[str, Field(pattern=SPACE_FORMAT_PATTERN)]] = Field(
        default_factory=list,
        description="Instance spaces that can be deleted through this view when using the data product.",
        max_length=100,
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
        min_length=1,
        max_length=50,
    )
    schema_space: str | None = Field(
        default=None,
        description="The schema space where the data product's data model and views are located. Defaults to the data product's externalId.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
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
    instance_scope: list[DataProductInstanceScope] | None = Field(
        default=None,
        description=(
            "The instance spaces that can be accessed through each view of the data product. "
            "Each view can appear at most once. Version views and role access filters must stay within this scope."
        ),
        max_length=100,
    )

    @model_validator(mode="after")
    def check_unique_scope_views(self) -> Self:
        seen: set[tuple[str, str]] = set()
        for scope in self.instance_scope or []:
            view = (scope.source.space, scope.source.external_id)
            if view in seen:
                raise ValueError(
                    f"Duplicate view in instanceScope: {view[0]}:{view[1]}. Each view can appear at most once."
                )
            seen.add(view)
        return self

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
