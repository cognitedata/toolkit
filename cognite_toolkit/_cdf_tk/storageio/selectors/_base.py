from abc import ABC, abstractmethod
from pathlib import Path

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.constants import DATA_MANIFEST_STEM
from cognite_toolkit._cdf_tk.utils.file import safe_write, sanitize_filename, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


class SelectorObject(BaseModel):
    """This is used as base class for all selector objects including nested ones."""

    model_config = ConfigDict(frozen=True, alias_generator=to_camel, populate_by_name=True)


class DataSelector(SelectorObject, ABC):
    """A selector gives instructions on what data to select from CDF.

    For example, for instances it can be a view or container, while for assets it can be a data set or asset subtree.
    """

    type: str
    kind: str | None = None

    def dump(self) -> dict[str, JsonVal]:
        return self.model_dump(by_alias=True)

    def dump_to_file(self, directory: Path) -> Path:
        """Dumps the selector to a YAML file in the specified directory.

        The filename is derived from the string representation of the selector.

        Args:
            directory: The directory where the YAML file will be saved.
        """

        filepath = directory / f"{sanitize_filename(str(self))}.{DATA_MANIFEST_STEM}.yaml"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        safe_write(file=filepath, content=yaml_safe_dump(self.model_dump(mode="json", by_alias=True)), encoding="utf-8")
        return filepath

    @property
    @abstractmethod
    def group(self) -> str:
        """A string representing the group of the selector, used for organizing files.

        It is used when downloading to determine the subdirectory within the output directory.

        For example, for raw table, the group would be the database name, while the selector itself
        would be the table name.
        """
        raise NotImplementedError()

    @property
    def display_name(self) -> str | None:
        """A human-readable name for the selector."""
        return None

    def __str__(self) -> str:
        # We want to force subclasses to implement __str__
        raise NotImplementedError()
