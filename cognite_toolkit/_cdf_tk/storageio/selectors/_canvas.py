from abc import ABC
from typing import Literal

from ._base import DataSelector


class CanvasSelector(DataSelector, ABC):
    kind: Literal["IndustrialCanvas"] = "IndustrialCanvas"
