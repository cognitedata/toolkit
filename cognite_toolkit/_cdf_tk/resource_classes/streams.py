from typing import Literal

from cognite_toolkit._cdf_tk.constants import StreamTemplateName

from .base import ToolkitResource


class StreamYAML(ToolkitResource):
    """Stream YAML resource class."""

    external_id: str
    settings: dict[Literal["template"], dict[Literal["name"], StreamTemplateName]]
