from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

from .base import BaseModelResource, ToolkitResource


class RawTableReference(BaseModelResource):
    database_name: str = Field(description="The name of the RAW database.")
    table_name: str = Field(description="The name of the RAW table.")


class ConsoleOwner(BaseModelResource):
    name: str = Field(description="The name of the owner.")
    email: str = Field(description="The email of the owner.")


class ConsoleExtractors(BaseModelResource):
    accounts: list[str] = Field(default_factory=list, description="The extractor accounts writing to the data set.")


class TransformationReference(BaseModelResource):
    name: str | int = Field(
        description="The transformation ID (for type 'jetfire') or a name for external transformations."
    )
    type: Literal["jetfire", "external"] = Field(description="The type of transformation.")
    details: str | None = Field(default=None, description="Details about the transformation.")


class ConsoleSource(BaseModelResource):
    names: list[str] = Field(default_factory=list, description="The names of the sources of the data set.")


class ConsoleAdditionalDoc(BaseModelResource):
    type: Literal["file", "url"] = Field(description="The type of the documentation.")
    id: str = Field(description="The file ID (for type 'file') or the URL (for type 'url').")
    name: str = Field(description="The display name of the documentation.")


class CatalogDataSetYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    name: str | None = Field(
        default=None,
        description="The name of the data set.",
        min_length=1,
        max_length=50,
    )
    description: str | None = Field(
        default=None,
        description="The description of the data set.",
        min_length=1,
        max_length=500,
    )
    write_protected: bool | None = Field(
        default=False,
        description="To write data to a write-protected data set.",
    )
    raw_tables: list[RawTableReference] | None = Field(
        default=None, description="The RAW tables that are part of the data set."
    )
    archived: bool | None = Field(default=None, description="Whether the data set is archived.")
    console_governed: bool | None = Field(default=None, description="Whether the data set is governed.")
    console_owners: list[ConsoleOwner] | None = Field(default=None, description="The owners of the data set.")
    console_extractors: ConsoleExtractors | None = Field(
        default=None, description="The extractors writing to the data set."
    )
    transformations: list[TransformationReference] | None = Field(
        default=None, description="The transformations writing to the data set."
    )
    console_source: ConsoleSource | None = Field(default=None, description="The sources of the data set.")
    console_additional_docs: list[ConsoleAdditionalDoc] | None = Field(
        default=None, description="Additional documentation for the data set."
    )

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
