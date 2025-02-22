from __future__ import annotations

import dataclasses
import itertools
import warnings
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Collection, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from functools import total_ordering
from typing import Any, ClassVar, Generic, TypeVar

from rich import print
from rich.console import Console

RICH_WARNING_FORMAT = "    [bold yellow]WARNING:[/] "
RICH_WARNING_DETAIL_FORMAT = f"{'    ' * 2}"


class SeverityLevel(Enum):
    ERROR = "error"
    HIGH = "red"
    MEDIUM = "yellow"
    LOW = "green"

    @property
    def prefix(self) -> str:
        if self == SeverityLevel.ERROR:
            return f"[bold red]ERROR [{self.name}]:[/]"
        else:
            return f"[bold {self.value}]WARNING [{self.name}]:[/]"

    @property
    def prefix_length(self) -> int:
        return len(self.prefix.split("]", 1)[1].rsplit("[", 1)[0])


@total_ordering
@dataclass(frozen=True)
class ToolkitWarning(ABC, UserWarning):
    severity: ClassVar[SeverityLevel]

    def group_key(self) -> tuple[Any, ...]:
        """This is used to group warnings together when printing them out."""
        return (type(self).__name__,)

    def group_header(self) -> str:
        """This can be overridden to provide a custom header for a group of warnings."""
        return f"    {type(self).__name__}:"

    def as_tuple(self) -> tuple[Any, ...]:
        return type(self).__name__, *dataclasses.astuple(self)

    def __lt__(self, other: ToolkitWarning) -> bool:
        if not isinstance(other, ToolkitWarning):
            return NotImplemented
        return self.as_tuple() < other.as_tuple()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ToolkitWarning):
            return NotImplemented
        return self.as_tuple() == other.as_tuple()

    @abstractmethod
    def get_message(self) -> str:
        raise NotImplementedError()

    def __str__(self) -> str:
        return self.get_message()

    def print_prepare(self) -> tuple[str, str]:
        prefix = self.severity.prefix
        end = "\n" + " " * ((self.severity.prefix_length + 1) // 2)
        message = self.get_message().replace("\n", end)
        return prefix, message

    def print_warning(self, include_timestamp: bool = False, console: Console | None = None) -> None:
        parts: tuple[str, ...] = self.print_prepare()
        if include_timestamp:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
            parts = (timestamp, *parts)
        if console is None:
            print(*parts)
        else:
            console.print(*parts)


T_Warning = TypeVar("T_Warning", bound=ToolkitWarning)


class WarningList(UserList, Generic[T_Warning]):
    def __init__(self, collection: Collection[T_Warning] | None = None) -> None:
        super().__init__(collection or [])

    def __str__(self) -> str:
        output = [""]
        for group_key, group in itertools.groupby(sorted(self), key=lambda w: w.group_key()):
            group_list = list(group)
            header = group_list[0].group_header()
            if header:
                output.append(header)
            for warning in group_list:
                output.append(f"{'    ' * 2} * {warning!s}")
        return "\n".join(output)


@dataclass(frozen=True)
class GeneralWarning(ToolkitWarning, ABC):
    severity: ClassVar[SeverityLevel]
    message: ClassVar[str | None] = None


@contextmanager
def catch_warnings(warning_type: type[ToolkitWarning] | None = None) -> Iterator[WarningList[ToolkitWarning]]:
    """Catch warnings and append them to the warning list."""
    warning_list = WarningList[ToolkitWarning]()
    with warnings.catch_warnings(record=True) as warning_logger:
        try:
            yield warning_list
        finally:
            for warning in warning_logger:
                if warning_type is None or isinstance(warning.message, warning_type):
                    warning_list.append(warning.message)
                elif isinstance(warning.message, ToolkitWarning):
                    warning.message.print_warning()
                else:
                    warnings.warn(warning.message, stacklevel=2)
