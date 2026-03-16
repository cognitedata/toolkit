from typing import Literal

from pydantic import Field

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId
from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, SPACE_FORMAT_PATTERN
from cognite_toolkit._cdf_tk.utils import humanize_collection

from ._base import DataSelector, SelectorObject


class SelectedStream(SelectorObject):
    """Selected stream for records."""

    external_id: str


class SelectedContainer(SelectorObject):
    """Selected container for records."""

    space: str = Field(
        description="Id of the space that the container belongs to.",
        min_length=1,
        max_length=43,
        pattern=SPACE_FORMAT_PATTERN,
    )
    external_id: str = Field(
        description="External-id of the container.",
        min_length=1,
        max_length=255,
        pattern=DM_EXTERNAL_ID_PATTERN,
    )

    def as_id(self) -> ContainerId:
        return ContainerId(space=self.space, external_id=self.external_id)

    def __str__(self) -> str:
        return f"{self.space}_{self.external_id}"


class RecordContainerSelector(DataSelector):
    type: Literal["recordContainer"] = "recordContainer"
    kind: Literal["Records"] = "Records"
    stream: SelectedStream
    container: SelectedContainer
    instance_spaces: tuple[str, ...] | None = None
    initialize_cursor: str | None = None

    def __str__(self) -> str:
        return f"{self.container.space}_{self.container.external_id}"

    @property
    def display_name(self) -> str:
        message = f"{self.kind.lower()} in stream {self.stream} with data in {self.container!s}"
        if self.instance_spaces:
            message += f" with {humanize_collection(self.instance_spaces)} instance spaces"
        if self.initialize_cursor:
            message += f" and initialize cursor {self.initialize_cursor}"
        return message
