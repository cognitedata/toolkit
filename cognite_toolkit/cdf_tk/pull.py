from __future__ import annotations

from typing import Any

import yaml

from cognite_toolkit.cdf_tk.utils import YAMLComment, YAMLWithComments


class ResourceYAML(YAMLWithComments[str, Any]):
    """This represents a YAML file that contains a single CDF resource such as transformation.

    It is used to load and dump an YAML file that contains comments.
    """

    def __init__(
        self, items: dict[str, Any] | None = None, comments: dict[tuple[str, ...], YAMLComment] | None = None
    ) -> None:
        super().__init__(items or {})
        self._comments = comments or {}

    def _get_comment(self, key: tuple[str, ...]) -> YAMLComment | None:
        return self._comments.get(key)

    @classmethod
    def load(cls, content: str) -> ResourceYAML:
        comments = cls._extract_comments(content)
        items = yaml.safe_load(content)
        if not isinstance(items, dict):
            raise ValueError(f"Expected a dictionary, got {type(items)}")
        return cls(items, comments)

    def dump(self) -> dict[str, Any]:
        return self.data

    def dump_yaml_with_comments(self, indent_size: int = 2) -> str:
        """Dump a config dictionary to a yaml string"""
        return self._dump_yaml_with_comments(indent_size, False)
