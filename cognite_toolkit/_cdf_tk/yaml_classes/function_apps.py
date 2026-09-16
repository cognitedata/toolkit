from pydantic import Field, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN

from .base import ToolkitResource


class FunctionAppsYAML(ToolkitResource):
    external_id: str = Field(description="The external ID provided by the client.", max_length=255)
    name: str = Field(description="The name of the function app.", max_length=140)
    description: str | None = Field(
        default=None, description="The description of the function app.", min_length=1, max_length=500
    )
    function_path: str | None = Field(
        default=None, description="Relative path to the file containing the handle function.", max_length=500
    )
    secrets: dict[str, str] | None = Field(
        default=None, description="Secrets attached to the function app.", max_length=30
    )
    env_vars: dict[str, str] | None = Field(
        default=None, description="User specified environment variables on the function app.", max_length=100
    )
    cpu: float | None = Field(default=None, description="Number of CPU cores per function app.")
    memory: float | None = Field(default=None, description="Memory per function app measured in GB.")
    runtime: str | None = Field(default=None, description="Runtime of the function app.")
    metadata: dict[str, str] | None = Field(
        default=None, description="Custom, application-specific metadata.", max_length=16
    )
    index_url: str | None = Field(default=None, description="A different Python package index.")
    extra_index_urls: list[str] | None = Field(default=None, description="Extra Python package indexes.")
    data_set_external_id: str | None = Field(
        default=None, description="Dataset external ID for the uploaded code file.", max_length=255
    )
    space: str | None = Field(
        default=None,
        description="Space for the CogniteFile containing uploaded code.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )

    @model_validator(mode="before")
    @classmethod
    def reject_owner(cls, value: object) -> object:
        if isinstance(value, dict) and "owner" in value:
            raise ValueError("FunctionApp does not support 'owner'; remove this field from the YAML resource.")
        return value

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
