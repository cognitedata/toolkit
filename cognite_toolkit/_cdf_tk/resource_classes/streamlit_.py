from typing import Literal

from pydantic import Field

from .base import ToolkitResource


class StreamlitYAML(ToolkitResource):
    external_id: str = Field(
        max_length=255,
        description="The external ID of the Streamlit app.",
    )
    name: str = Field(
        description="The name of the Streamlit app.",
        min_length=1,
        max_length=255,
    )
    creator: str = Field(description="The creator of the Streamlit app.")
    entrypoint: str | None = Field(None, description="Path to the entrypoint file of the Streamlit app.")
    description: str | None = Field(None, description="The description of the Streamlit app.")
    published: bool = Field(False, description="Whether the Streamlit app is published or not.")
    theme: Literal["Light", "Dark"] = Field("Light", description="The theme of the Streamlit app.")
    thumbnail: str | None = Field(None, description="URL to the thumbnail image of the Streamlit app.")
    data_set_external_id: str | None = Field(
        None, description="The external ID of the data set to associate with the app."
    )
