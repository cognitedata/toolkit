from typing import Literal

from pydantic import Field

from .base import ToolkitResource


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
    secrets: dict[str, str] | None = Field(
        default=None,
        description="Secrets attached to the function.",
        max_length=30,
    )
    env_vars: dict[str, str] | None = Field(
        default=None,
        description="User specified environment variables on the function.",
        max_length=100,
    )
    cpu: float | None = Field(default=None, description="Number of CPU cores per function.")
    memory: float | None = Field(default=None, description="Memory per function measured in GB.")
    runtime: Literal["py39", "py310", "py311"] | None = Field(default="py311", description="Runtime of the function.")
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
