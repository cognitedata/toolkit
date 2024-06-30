from __future__ import annotations

import dataclasses
import itertools
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Collection
from dataclasses import dataclass
from enum import Enum
from functools import total_ordering
from typing import Any, ClassVar, Generic, TypeVar

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
class ToolkitWarning(ABC):
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
