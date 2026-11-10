from typing import Literal

from ._base import DataSelector


class SequenceRowSelector(DataSelector):
    type: Literal["sequenceRows"] = "sequenceRows"
    kind: Literal["SequenceRows"] = "SequenceRows"
    external_id: str

    def __str__(self) -> str:
        return self.external_id
