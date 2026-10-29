from pathlib import Path
from typing import TYPE_CHECKING, Literal, get_args

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible

from .base import BaseModelResource, ToolkitResource

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildWarning


StreamTemplate = Literal["ImmutableTestStream", "BasicArchive", "BasicLiveData"]
AVAILABLE_STREAM_TEMPLATES = get_args(StreamTemplate)


class StreamTemplateRef(BaseModelResource):
    name: StreamTemplate | str


class StreamSettings(BaseModelResource):
    """Stream settings resource class."""

    template: StreamTemplateRef = Field(
        description="Reference to a template which should be used to define initial settings for the stream."
    )


class StreamYAML(ToolkitResource):
    """Stream YAML resource class."""

    external_id: str = Field(
        description="The external ID of the stream.",
        min_length=1,
        max_length=100,
        pattern="^[a-z]([a-z0-9_-]{0,98}[a-z0-9])?$",
    )
    settings: StreamSettings = Field(
        description="Stream settings which should be applied to a stream.",
    )

    def syntax_warnings(self, source_file: Path) -> "list[BuildWarning]":
        if self.settings.template.name not in AVAILABLE_STREAM_TEMPLATES:
            from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildWarning

            return [
                BuildWarning(
                    source_file=source_file,
                    code="INVALID-VALUE",
                    message=f"Stream template '{self.settings.template.name}' is not an officially supported template.\n"
                    f"Note if you are using a custom template, this warning can be ignored.\n"
                    f"To suppress this warning, add the following to you the top of"
                    f"{relative_to_if_possible(source_file).as_posix()}:\n  `# rules: ignore[INVALID-VALUE]` ",
                    fix=f"Please use one of the following templates: {', '.join(AVAILABLE_STREAM_TEMPLATES)}",
                )
            ]
        return []

    def as_id(self) -> ExternalId:
        return ExternalId(external_id=self.external_id)
