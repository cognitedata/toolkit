import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from rich import print
from rich.console import Console

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.data_classes import CommandTracking
from cognite_toolkit._cdf_tk.tk_warnings import (
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.tracker import Tracker

CDF_TOML = CDFToml.load(Path.cwd())


class ToolkitCommand:
    def __init__(
        self,
        print_warning: bool = True,
        skip_tracking: bool = False,
        silent: bool = False,
        client: ToolkitClient | None = None,
    ):
        self._print_warning = print_warning
        self.silent = silent
        self.warning_list = WarningList[ToolkitWarning]()
        self.tracker = Tracker(skip_tracking)
        self._client = client
        cmd = type(self).__name__.removesuffix("Command")
        event_name = f"command{cmd.capitalize()}"
        self._additional_tracking_info = CommandTracking(event_name=event_name)

    @property
    def print_warning(self) -> bool:
        return self._print_warning and not self.silent

    def _track_command(self, error: Exception | None) -> None:
        subcommands = _parse_sys_args(_collect_known_commands())
        command = self._additional_tracking_info

        command.warning_total_count = len(self.warning_list)
        command.result = "success" if error is None else type(error).__name__
        command.subcommands = subcommands
        command.alpha_flags = [name for name, value in CDF_TOML.alpha_flags.items() if value]
        command.plugins = [name for name, value in CDF_TOML.plugins.items() if value]
        self.tracker.track(command, self._client)

    def run(self, execute: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        try:
            result = execute(*args, **kwargs)
        except Exception as e:
            self._track_command(e)
            raise e
        else:
            self._track_command(error=None)
        return result

    def warn(self, warning: ToolkitWarning, include_timestamp: bool = False, console: Console | None = None) -> None:
        self.warning_list.append(warning)
        if self.print_warning:
            warning.print_warning(include_timestamp, console)

    def console(self, message: str, prefix: str = "[bold green]INFO:[/] ") -> None:
        if not self.silent:
            print(f"{prefix}{message}")


def _parse_sys_args(known_commands: frozenset[str]) -> list[str]:
    return [arg for arg in sys.argv[1:] if arg in known_commands]


def _collect_known_commands() -> frozenset[str]:
    """Collect all registered CLI command names by introspecting the loaded Typer app.

    Uses sys.modules to avoid a circular import — the app module is always loaded
    before tracking runs, so no explicit import is needed here.
    """
    module = sys.modules.get("cognite_toolkit._cdf")
    if module is None:
        return frozenset()
    try:
        import typer.main as typer_main

        app = getattr(module, "_app", None)
        if app is None:
            return frozenset()
        names: set[str] = set()
        _collect_click_command_names(typer_main.get_command(app), names)
        return frozenset(names)
    except (ImportError, AttributeError):
        return frozenset()


def _collect_click_command_names(group: Any, names: set[str]) -> None:
    if hasattr(group, "commands"):
        for name, cmd in group.commands.items():
            names.add(name)
            _collect_click_command_names(cmd, names)
