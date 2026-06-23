import os
import sys
from dataclasses import dataclass
from enum import Enum

import questionary
import typer
from rich import print
from rich.markup import escape

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import HINT_LEAD_TEXT, URL, clean_name
from cognite_toolkit._cdf_tk.ui import QUESTIONARY_STYLE


@dataclass
class Plugin:
    name: str
    description: str

    def is_enabled(self) -> bool:
        return CDFToml.load().plugins.get(clean_name(self.name), False)


class Plugins(Enum):
    run = Plugin("run", "plugin for Run command to execute Python scripts in CDF")
    dump = Plugin("dump", "plugin for Dump command to retrieve Asset resources from CDF")
    dev = Plugin("dev", "plugin for commands to develop modules in CDF")
    data = Plugin("data", "plugin for Data command to manage data in CDF")

    @staticmethod
    def list() -> dict[str, bool]:
        res = {plugin.name: Plugin.is_enabled(plugin.value) for plugin in Plugins}
        return res

    def ensure_enabled(self) -> None:
        """Ensure this plugin is enabled before running a command that requires it.

        Returns immediately if the plugin is already enabled. Otherwise the user is prompted
        to enable it. On confirmation the plugin is enabled in ``cdf.toml`` and execution
        continues; otherwise the manual hint is printed and the command is aborted with
        ``typer.Exit``. In non-interactive shells (e.g. CI) the command fails loudly by
        default; set the ``CDF_ASSUME_YES`` environment variable to auto-enable instead.
        """
        plugin = self.value
        if plugin.is_enabled():
            return
        name = clean_name(plugin.name)
        if _confirm_enable(name) and CDFToml.enable_plugin(name):
            print(f"{HINT_LEAD_TEXT} Enabled plugin [bold]{name}[/bold] in [bold]cdf.toml[/bold].")
            return
        _print_disabled_hint(name)
        raise typer.Exit(code=1)


def _confirm_enable(plugin_name: str) -> bool:
    """Ask the user whether to enable the plugin.

    Honours the ``CDF_ASSUME_YES`` environment variable for non-interactive/CI use, and
    returns ``False`` in non-interactive shells so that CI fails loudly by default.
    """
    if os.environ.get("CDF_ASSUME_YES", "").strip().lower() in ("1", "true", "yes"):
        return True
    if not (sys.stdin.isatty() and sys.stdout.isatty()):
        return False
    try:
        return bool(
            questionary.confirm(
                f"The plugin '{plugin_name}' is required for this command but is not enabled. Enable it now?",
                default=True,
                style=QUESTIONARY_STYLE,
            ).unsafe_ask()
        )
    except KeyboardInterrupt:
        return False


def _print_disabled_hint(plugin_name: str) -> None:
    section = r"[plugins]"
    print(
        f"{HINT_LEAD_TEXT} The plugin [bold]{plugin_name}[/bold] is not enabled."
        f"\nEnable it in the [bold]cdf.toml[/bold] file by setting '{plugin_name} = true' in the "
        f"[bold]{escape(section)}[/bold] section."
        f"\nDocs to learn more: [blue][link={URL.plugins}]{URL.plugins}[/link][/blue]"
    )
