from typing import Literal

from ._base import DataSelector


class ThreeDSelector(DataSelector):
    type: Literal["3D"] = "3D"
    kind: Literal["3D"] = "3D"
    published: bool | None = None

    @property
    def group(self) -> str:
        return "3DModels"

    def __str__(self) -> str:
        if self.published is not None:
            return f"3DModels_published_{self.published}"
        return "3DModels_all"
