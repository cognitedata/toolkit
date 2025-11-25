import hashlib
from abc import ABC
from typing import Literal

from ._base import DataSelector


class CanvasSelector(DataSelector, ABC):
    kind: Literal["IndustrialCanvas"] = "IndustrialCanvas"


class CanvasExternalIdSelector(CanvasSelector):
    type: Literal["canvasExternalId"] = "canvasExternalId"
    external_ids: tuple[str, ...]

    @property
    def group(self) -> str:
        return "Canvas"

    def __str__(self) -> str:
        hash_ = hashlib.md5(",".join(sorted(self.external_ids)).encode()).hexdigest()[:8]
        return f"canvas_count_{len(self.external_ids)}_hash_{hash_}"
