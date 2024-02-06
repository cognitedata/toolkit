from __future__ import annotations

from collections import UserDict
from typing import Any

from cognite_toolkit.cdf_tk.utils import YAMLComment


class ResourceYAML(UserDict[str, Any]):

    def __init__(self, items: dict[str, Any] | None = None) -> None:
        super().__init__(items or {})
        self.comments: dict[tuple[str, ...], YAMLComment] = {}

    @classmethod
    def load(cls, content: str) -> ResourceYAML:
        raise NotImplementedError

    def dump(self) -> dict[str, Any]:
        raise NotImplementedError

    def dump_with_comments(self) -> str:
        raise NotImplementedError
