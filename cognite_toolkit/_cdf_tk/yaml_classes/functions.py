from typing import Annotated, Self

from pydantic import AfterValidator, Field, model_validator

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.constants import SPACE_FORMAT_PATTERN
from cognite_toolkit._cdf_tk.utils import humanize_collection

from .base import ToolkitResource

FORBIDDEN_SECRET_KEYS = frozenset({"token", "indexUrl", "extraIndexUrls"})
SECRET_KEY_PATTERN = r"^[a-z0-9-]+$"


def _validate_secret_key(value: str) -> str:
    if value in FORBIDDEN_SECRET_KEYS:
        raise ValueError(
            f"'{value}' is a reserved secret name. Reserved names are: {humanize_collection(FORBIDDEN_SECRET_KEYS)}."
        )
    return value


SecretKey = Annotated[
    str,
    Field(
        pattern=SECRET_KEY_PATTERN,
        min_length=1,
        max_length=15,
        description="Secret key name (lower case letters, numbers, and hyphens only, 1-15 characters).",
    ),
    AfterValidator(_validate_secret_key),
]

EnvVarKey = Annotated[
    str,
    Field(
        pattern=r"^[a-zA-Z0-9_]+$",
        description="Environment variable key name (letters, numbers, and underscores only).",
    ),
]


class FunctionsYAML(ToolkitResource):
    external_id: str = Field(
        description="The external ID provided by the client.",
        max_length=255,
    )
    name: str = Field(
        description="The name of the function.",
        max_length=140,
    )
    description: str | None = Field(
        default=None,
        description="The description of the function.",
        min_length=1,
        max_length=500,
    )
    owner: str | None = Field(
        default=None,
        description="Owner of the function.",
        max_length=128,
    )
    function_path: str | None = Field(
        default=None,
        description="Relative path from the root folder to the file containing the handle function.",
        max_length=500,
    )
    secrets: dict[SecretKey, str] | None = Field(
        default=None,
        description="Secrets attached to the function.",
        max_length=30,
    )
    env_vars: dict[EnvVarKey, str] | None = Field(
        default=None,
        description="User specified environment variables on the function.",
        max_length=100,
    )
    cpu: float | None = Field(default=None, description="Number of CPU cores per function.")
    memory: float | None = Field(default=None, description="Memory per function measured in GB.")
    runtime: str | None = Field(
        default=None,
        description="Runtime of the function. Defaults to the CDF project's default runtime if not set.",
    )
    metadata: dict[str, str] | None = Field(
        default=None, description="Custom, application-specific metadata.", max_length=16
    )
    index_url: str | None = Field(
        default=None,
        description="Specify a different python package index, allowing for packages published in private repositories.",
    )
    extra_index_urls: list[str] | None = Field(
        default=None,
        description=" Extra package index URLs to use when building the function, allowing for packages published in private repositories.",
    )
    data_set_external_id: str | None = Field(
        default=None,
        description="Target dataset external ID for the file where the function code is stored.",
        max_length=255,
    )
    space: str | None = Field(
        default=None,
        description="The space for the CogniteFile where the function code is stored.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )

    @model_validator(mode="after")
    def validate_code_storage(self) -> Self:
        """The function code is uploaded as a file, which must be stored in either a data set or a space."""
        if self.data_set_external_id is None and self.space is None:
            raise ValueError("Either dataSetExternalId or space must be set, to store the function code.")
        if self.data_set_external_id is not None and self.space is not None:
            raise ValueError("Only one of dataSetExternalId or space can be set, to store the function code.")
        return self

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
