from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from cognite_toolkit._cdf_tk.utils.file import safe_write, sanitize_filename, yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


@dataclass(frozen=True)
class DataSelector(ABC):
    """A selector gives instructions on what data to select from CDF.

    For example, for instances it can be a view or container, while for assets it can be a data set or asset subtree.
    """

    @abstractmethod
    def dump(self) -> dict[str, JsonVal]:
        raise NotImplementedError()

    def dump_to_file(self, directory: Path) -> None:
        """Dumps the selector to a YAML file in the specified directory.

        The filename is derived from the string representation of the selector.

        Args:
            directory: The directory where the YAML file will be saved.
        """

        filepath = directory / f"{sanitize_filename(str(self))}.Selector.yaml"
        safe_write(filepath, yaml_safe_dump(self.dump()))

    def __str__(self) -> str:
        # We want to force subclasses to implement __str__
        raise NotImplementedError()
