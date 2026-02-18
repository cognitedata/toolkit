from typing import Literal

from cognite.client.data_classes.data_modeling import ContainerId
from pydantic import Field

from cognite_toolkit._cdf_tk.constants import DM_EXTERNAL_ID_PATTERN, SPACE_FORMAT_PATTERN

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

    @property
    def group(self) -> str:
        return self.stream.external_id

    def __str__(self) -> str:
        return f"{self.container.space}_{self.container.external_id}"
