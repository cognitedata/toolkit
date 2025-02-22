from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any

from rich import print
from rich.console import Console

from cognite_toolkit._cdf_tk.tk_warnings import (
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.tracker import Tracker

_HAS_PRINTED_COLLECT_MESSAGE = False


class ToolkitCommand:
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False):
        self._print_warning = print_warning
        self.silent = silent
        self.warning_list = WarningList[ToolkitWarning]()
        self.tracker = Tracker(skip_tracking)

    @property
    def print_warning(self) -> bool:
        return self._print_warning and not self.silent

    def _track_command(self, result: str | Exception) -> None:
        self.tracker.track_cli_command(self.warning_list, result, type(self).__name__.removesuffix("Command"))

    def run(self, execute: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        global _HAS_PRINTED_COLLECT_MESSAGE
        is_collect_command = len(sys.argv) >= 2 and "collect" == sys.argv[1]
        if (
            not self.tracker.opted_in
            and not self.tracker.opted_out
            and not is_collect_command
            and not _HAS_PRINTED_COLLECT_MESSAGE
        ):
            print(
                "You acknowledge and agree that the CLI tool may collect usage information, user environment, "
                "and crash reports for the purposes of providing services of functions that are relevant "
                "to use of the CLI tool and product improvements. "
                "To remove this message run 'cdf collect opt-in', "
                "or to stop collecting usage information run 'cdf collect opt-out'."
            )
            _HAS_PRINTED_COLLECT_MESSAGE = True

        try:
            result = execute(*args, **kwargs)
        except Exception as e:
            self._track_command(e)
            raise e
        else:
            self._track_command("success")
        return result

    def warn(self, warning: ToolkitWarning, include_timestamp: bool = False, console: Console | None = None) -> None:
        self.warning_list.append(warning)
        if self.print_warning:
            warning.print_warning(include_timestamp, console)

    def console(self, message: str, prefix: str = "[bold green]INFO:[/] ") -> None:
        if not self.silent:
            print(f"{prefix}{message}")
