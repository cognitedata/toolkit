from typing import TypeAlias

from cognite_toolkit._cdf_tk.utils.file import YAMLComment

yaml_key: TypeAlias = tuple[str | int, ...]


class YAMLComments:
    def __init__(self, comments: dict[yaml_key, YAMLComment]) -> None:
        self.comments = comments

    @classmethod
    def load(cls, yaml_str: str) -> "YAMLComments":
        raise NotImplementedError()

    def dump(self, yaml_str: str) -> str:
        raise NotImplementedError()
