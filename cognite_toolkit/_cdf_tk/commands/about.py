import platform
import sys
from pathlib import Path

from rich import print
from rich.markup import escape
from rich.table import Table

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml, _read_toml
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import clean_name
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.plugins import Plugins
from cognite_toolkit._version import __version__


class AboutCommand(ToolkitCommand):
    def execute(self) -> None:
        # Version information
        print(f"\n[bold cyan]Cognite Toolkit[/bold cyan] version: [yellow]{__version__}[/yellow]")
        print(f"Python version: {sys.version.split()[0]}")
        print(f"Platform: {platform.system()} {platform.release()}")

        # Check for cdf.toml in the current directory
        cwd = Path.cwd()
        cdf_toml_path = cwd / CDFToml.file_name

        if cdf_toml_path.exists():
            print(f"\n[bold green]Configuration file found:[/bold green] {cdf_toml_path}")

            cdf_toml = CDFToml.load(cwd)

            # We need to read the raw TOML to get original key names for plugins and alpha flags
            raw_toml = _read_toml(cdf_toml_path)

            self._check_unrecognized_sections(raw_toml)
            self._display_plugins(cdf_toml, raw_toml)
            self._display_alpha_flags(cdf_toml, raw_toml)
            self._display_additional_config(cdf_toml)

        else:
            # Search for cdf.toml in subdirectories
            found_files = self._search_cdf_toml(cwd)

            if found_files:
                print(f"\n[bold yellow]No cdf.toml found in current directory:[/bold yellow] {cwd}")
                print("\n[bold]Found cdf.toml files in subdirectories:[/bold]")
                for file in found_files:
                    rel_path = file.relative_to(cwd)
                    print(f"  • {rel_path}")
                print(f"\n[bold cyan]Hint:[/bold cyan] Move one of these files to {cwd} or navigate to its directory.")
            else:
                print("\n[bold yellow]No cdf.toml found[/bold yellow] in current directory or subdirectories.")
                print(f"Current directory: {cwd}")
                print("\n[bold cyan]Hint:[/bold cyan] Run [yellow]cdf init[/yellow] to create a new project.")

    def _check_unrecognized_sections(self, raw_toml: dict) -> None:
        """Check for unrecognized tables in cdf.toml."""
        # Valid top-level tables in cdf.toml
        valid_tables = {"cdf", "modules", "alpha_flags", "feature_flags", "plugins", "library"}

        # Filter out empty keys, whitespace-only keys, and check for unrecognized tables
        unrecognized_tables = [key for key in raw_toml.keys() if key and key.strip() and key not in valid_tables]

        if unrecognized_tables:
            print(
                "\n[bold yellow]Warning:[/bold yellow] The following tables in cdf.toml are not recognized and will have no effect:"
            )
            for table in unrecognized_tables:
                # Escape the table name and build the message
                escaped_table = escape(f"[{table}]")
                print(f"  • {escaped_table} ([red]unused[/red])")

                # Try to find a matching valid table by stripping non-alphabetical characters
                suggestion = self._find_similar_table(table, valid_tables)
                if suggestion:
                    escaped_suggestion = escape(f"[{suggestion}]")
                    print(f"    [dim]Hint: Did you mean {escaped_suggestion}?[/dim]")

    @staticmethod
    def _find_similar_table(unrecognized: str, valid_tables: set[str]) -> str | None:
        """Find a similar valid table by comparing alphabetical characters only."""
        # Keep only alphabetical characters and lowercase
        normalized_unrecognized = "".join(c for c in unrecognized if c.isalpha()).lower()

        # First, try exact match (after normalization)
        for valid in valid_tables:
            normalized_valid = "".join(c for c in valid if c.isalpha()).lower()
            if normalized_unrecognized == normalized_valid:
                return valid

        # If no match, check for singular/plural variations (missing/extra 's')
        for valid in valid_tables:
            normalized_valid = "".join(c for c in valid if c.isalpha()).lower()

            # Check if adding 's' to unrecognized matches valid (e.g., "plugin" -> "plugins")
            if normalized_unrecognized + "s" == normalized_valid:
                return valid

        return None

    def _display_plugins(self, cdf_toml: CDFToml, raw_toml: dict) -> None:
        """Display all available plugins and their status."""
        table = Table(title="Plugins", show_header=True)
        table.add_column("Plugin", justify="left", style="cyan")
        table.add_column("Status", justify="center")
        table.add_column("Description", justify="left")

        # Track which plugins we've seen
        seen_plugins = set()

        # Show all plugins from the enum
        for plugin in Plugins:
            plugin_name = plugin.value.name
            cleaned_key = clean_name(plugin_name)
            seen_plugins.add(cleaned_key)

            is_enabled = cdf_toml.plugins.get(cleaned_key, False)
            if is_enabled:
                status = "[green]✓ enabled[/green]"
            else:
                status = "[dim]○ disabled[/dim]"

            table.add_row(plugin_name, status, plugin.value.description)

        print()
        print(table)

        # Show any unrecognized plugins from cdf.toml using original key names
        raw_plugins = raw_toml.get("plugins", {})
        unrecognized = []
        for original_key, value in raw_plugins.items():
            cleaned_key = clean_name(original_key)
            if cleaned_key not in seen_plugins:
                unrecognized.append((original_key, value))

        if unrecognized:
            print(
                "\n[bold yellow]Warning:[/bold yellow] The following plugins in cdf.toml are not recognized and will have no effect:"
            )
            for original_key, is_enabled in unrecognized:
                status = "enabled" if is_enabled else "disabled"
                print(f"  • {original_key} ([red]unused[/red], {status})")

    def _display_alpha_flags(self, cdf_toml: CDFToml, raw_toml: dict) -> None:
        """Display available alpha flags and their status."""
        table = Table(title="Alpha Flags", show_header=True)
        table.add_column("Flag", justify="left", style="yellow")
        table.add_column("Status", justify="center")
        table.add_column("Description", justify="left")

        # Track which flags we've seen
        seen_flags = set()

        # Show flags from the enum that are either enabled or visible
        for flag in Flags:
            cleaned_key = clean_name(flag.name)
            seen_flags.add(cleaned_key)

            is_enabled = cdf_toml.alpha_flags.get(cleaned_key, False)

            # Only show if enabled or visible
            if is_enabled or flag.value.visible:
                # Convert enum name to kebab-case for display
                display_name = flag.name.lower().replace("_", "-")

                if is_enabled:
                    status = "[green]✓ enabled[/green]"
                else:
                    status = "[dim]○ disabled[/dim]"

                table.add_row(display_name, status, flag.value.description)

        print()
        print(table)

        # Show any unrecognized flags from cdf.toml using original key names
        raw_flags = raw_toml.get("alpha_flags", {})
        unrecognized = []
        for original_key, value in raw_flags.items():
            cleaned_key = clean_name(original_key)
            if cleaned_key not in seen_flags:
                unrecognized.append((original_key, value))

        if unrecognized:
            print(
                "\n[bold yellow]Warning:[/bold yellow] The following alpha flags in cdf.toml are not recognized and will have no effect:"
            )
            for original_key, is_enabled in unrecognized:
                status = "enabled" if is_enabled else "disabled"
                print(f"  • {original_key} ([red]unused[/red], {status})")

    def _display_additional_config(self, cdf_toml: CDFToml) -> None:
        """Display additional configuration information."""
        print("\n[bold]Additional Configuration:[/bold]")

        print(f"  Default environment: [cyan]{cdf_toml.cdf.default_env}[/cyan]")

        if cdf_toml.cdf.has_user_set_default_org:
            print(f"  Default organization dir: [cyan]{cdf_toml.cdf.default_organization_dir}[/cyan]")

        if cdf_toml.cdf.file_encoding:
            print(f"  File encoding: [cyan]{cdf_toml.cdf.file_encoding}[/cyan]")

        print(f"  Modules version: [cyan]{cdf_toml.modules.version}[/cyan]")

        if cdf_toml.libraries:
            print(f"  Configured libraries: [cyan]{len(cdf_toml.libraries)}[/cyan]")
            for lib_name, lib_config in cdf_toml.libraries.items():
                print(f"    • {lib_name}: [dim]{lib_config.url}[/dim]")

    def _search_cdf_toml(self, cwd: Path) -> list[Path]:
        """Search for cdf.toml files in immediate subdirectories (one level down)."""
        found_files = []

        try:
            # Search only immediate subdirectories (one level down)
            for item in cwd.iterdir():
                if item.is_dir():
                    potential_file = item / CDFToml.file_name
                    if potential_file.exists():
                        found_files.append(potential_file)
        except PermissionError:
            pass  # Skip directories we can't access

        return sorted(found_files)
