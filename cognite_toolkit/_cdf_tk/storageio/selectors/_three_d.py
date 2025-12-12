import hashlib
from abc import ABC
from typing import Literal

from ._base import DataSelector


class ThreeDSelector(DataSelector, ABC):
    kind: Literal["3D"] = "3D"

    @property
    def group(self) -> str:
        return "3DModels"


class ThreeDModelFilteredSelector(ThreeDSelector):
    type: Literal["3DFiltered"] = "3DFiltered"
    model_type: Literal["Classic", "DataModel"] = "Classic"
    published: bool | None = None

    def __str__(self) -> str:
        suffix = f"3DModels_{self.model_type}"
        if self.published is not None:
            return f"{suffix}_published_{self.published}"
        return suffix


class ThreeDModelIdSelector(ThreeDSelector):
    type: Literal["3DId"] = "3DId"
    ids: tuple[int, ...]

    def __str__(self) -> str:
        hash_ = hashlib.md5(",".join(sorted(map(str, self.ids))).encode()).hexdigest()[:8]
        return f"3DModels_ids_count_{len(self.ids)}_hash_{hash_}"
