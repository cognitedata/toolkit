from pathlib import Path
from typing import Annotated, ClassVar, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.utils.file import read_yaml_file, safe_write


class ProgressObject(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class BookmarkType(ProgressObject):
    type: str


class CursorBookmark(BookmarkType):
    type: Literal["cursor"] = "cursor"
    cursor: str

    def __str__(self) -> str:
        return f"cursor {self.cursor!r}"


class FileBookmark(BookmarkType):
    type: Literal["file"] = "file"
    lineno: int
    filepath: Path

    def __str__(self) -> str:
        return f"lineno {self.lineno:,} in {self.filepath.as_posix()!r}"


class NoBookmark(BookmarkType):
    type: Literal["nobookmark"] = "nobookmark"

    def __str__(self) -> str:
        return "beginning"


Bookmark = Annotated[CursorBookmark | FileBookmark | NoBookmark, Field(discriminator="type")]


class ProgressYAML(ProgressObject):
    file_suffix: ClassVar[Literal["Progress"]] = "Progress"
    status: Literal["in-progress", "completed", "failed", "stopped"]
    bookmarks: dict[str, Bookmark]
    total: int | None = None
    completed: int | None = None

    @classmethod
    def try_load(cls, directory: Path, filestem: str) -> "ProgressYAML | None":
        filepath = cls._get_filepath(directory, filestem)
        if not filepath.exists():
            return None
        return cls.model_validate(read_yaml_file(filepath))

    @classmethod
    def _get_filepath(cls, directory: Path, filestem: str) -> Path:
        return directory / f"{filestem}.{cls.file_suffix}.yaml"

    def dump_to_file(self, directory: Path, filestem: str) -> None:
        filepath = self._get_filepath(directory, filestem)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        safe_write(filepath, yaml.safe_dump(self.model_dump(by_alias=True)))

    def get_first_bookmark(self) -> Bookmark | None:
        if not self.bookmarks:
            return None
        return next((bookmark for bookmark in self.bookmarks.values() if bookmark.type != "nobookmark"), None)
