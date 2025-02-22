import re
from collections.abc import Iterator
from dataclasses import dataclass
from typing import TypeAlias

from cognite_toolkit._cdf_tk.utils.file import YAMLComment

yaml_key: TypeAlias = tuple[str | int, ...]


class YAMLComments:
    def __init__(self, comments: dict[yaml_key, YAMLComment]) -> None:
        self.comments = comments

    @classmethod
    def load(cls, yaml_str: str) -> "YAMLComments":
        return cls(_YAMLCommentParser(yaml_str).parse())

    def dump(self, yaml_str: str) -> str:
        return _YAMLCommentParser(yaml_str).dump(self.comments)


@dataclass
class _YAMLLine:
    indent: int
    key: str | None
    comment: str | None
    is_array: bool
    raw: str

    @property
    def is_comment_line(self) -> bool:
        return self.comment is not None and self.key is None


class _YAMLCommentParser:
    token_pattern = re.compile(r"[\w\n]+|[^\w\s]", flags=re.DOTALL)

    def __init__(self, yaml_str: str) -> None:
        self.yaml_str = yaml_str

    def _iterate_lines(self) -> Iterator[tuple[list[str | int], _YAMLLine]]:
        lines = self.yaml_str.splitlines()
        key: list[str | int] = []
        last_indent = 0
        indent_size: int | None = None
        index_key_level_by_indent: dict[int, tuple[int, int]] = {}
        for line_str in lines:
            line = self._parse_line(line_str)
            if indent_size is None and line.indent > 0:
                # Infer indentation size
                indent_size = line.indent

            if line.is_comment_line:
                ...
            elif line.is_array:
                if line.indent not in index_key_level_by_indent:
                    index, key_level = 0, len(key)
                else:
                    index, key_level = index_key_level_by_indent[line.indent]
                    index += 1
                index_key_level_by_indent[line.indent] = index, key_level
                key.append(index)
                if line.key:
                    key.append(line.key)
            elif line.key and line.indent > last_indent:
                key.append(line.key)
            elif line.key and line.indent < last_indent:
                if indent_size is None:
                    raise ValueError("Indentation size could not be determined")
                key = key[: line.indent // indent_size]
                key.append(line.key)
            elif line.key:
                if key:
                    key.pop()
                key.append(line.key)

            yield key, line
            if not line.is_comment_line:
                last_indent = line.indent

    def parse(self) -> dict[yaml_key, YAMLComment]:
        comments: dict[yaml_key, YAMLComment] = {}
        last_comment: list[str] = []
        for full_key, line in self._iterate_lines():
            if line.comment and line.key:
                # End-of-line comment
                comments[tuple(full_key)] = YAMLComment(after=[line.comment], above=last_comment)
                last_comment = []
            elif line.comment:
                last_comment.append(line.comment)
            elif line.key and last_comment:
                comments[tuple(full_key)] = YAMLComment(above=last_comment)
                last_comment = []

        return comments

    def dump(self, comments: dict[yaml_key, YAMLComment]) -> str:
        new_lines: list[str] = []
        for full_key, line in self._iterate_lines():
            if comment := comments.get(tuple(full_key)):
                for above_comment in comment.above:
                    new_lines.append(f"{' ' * line.indent}#{above_comment}")
                if comment.after:
                    after_comments = " ".join(comment.after)
                    new_lines.append(f"{line.raw} #{after_comments}")
                else:
                    new_lines.append(line.raw)
            else:
                new_lines.append(line.raw)
        return "\n".join(new_lines)

    def _parse_line(self, line_str: str) -> _YAMLLine:
        indent = len(line_str) - len(line_str.lstrip())
        is_array = False
        key: str | None = None
        comment: str | None = None
        in_single_quote = False
        in_double_quote = False
        for no, character in enumerate(line_str):
            if character == '"':
                in_double_quote = not in_double_quote
            elif character == "'":
                in_single_quote = not in_single_quote

            if in_single_quote or in_double_quote:
                continue

            if character == "#":
                comment = "".join(line_str[no + 1 :])
                break
            elif character == ":" and no > 0:
                key = "".join(line_str[:no]).lstrip()
            elif character == "-":
                is_array = True
                indent = len(line_str) - len(line_str.lstrip().lstrip("-").lstrip())

        return _YAMLLine(indent, key, comment, is_array, line_str)
