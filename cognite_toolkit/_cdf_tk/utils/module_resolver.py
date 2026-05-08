from pathlib import Path
from typing import cast

import questionary
import typer
from questionary import Choice
from rich import print

from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.utils.file import validate_safe_path


class ModuleResolver:
    @staticmethod
    def get_or_prompt_module_path(organization_dir: Path, module_name: str | None, verbose: bool = False) -> Path:
        """Return the path for the given module name, or prompt the user to select/create one.

        If module_name matches an existing module, returns its path directly.
        If module_name is given but not found, asks the user to confirm creating a new one.
        If module_name is None, shows an interactive list of existing modules plus a create option.
        """
        from cognite_toolkit._cdf_tk.data_classes import ModuleDirectories

        present_modules = ModuleDirectories.load(organization_dir, None)

        if module_name:
            for mod in present_modules:
                if mod.name.casefold() == module_name.casefold():
                    return mod.dir

            if questionary.confirm(f"{module_name} module not found. Do you want to create a new one?").unsafe_ask():
                validate_safe_path(module_name)
                return organization_dir / MODULES / module_name

            if verbose:
                print(f"[red]Aborting as {module_name} module not found...[/red]")
            else:
                print("[red]Aborting...[/red]")
            raise typer.Exit()

        choices = [Choice(title=mod.name, value=mod.dir) for mod in present_modules]
        choices.append(Choice(title="<Create new module>", value="NEW"))

        selected = questionary.select("Select a module:", choices=choices).unsafe_ask()

        if selected == "NEW":
            new_module_name = questionary.text("Enter name for new module:").unsafe_ask()
            if not new_module_name:
                print("[red]No module name provided. Aborting...[/red]")
                raise typer.Exit()
            validate_safe_path(new_module_name)
            return organization_dir / MODULES / new_module_name

        if not selected:
            print("[red]No module selected. Aborting...[/red]")
            raise typer.Exit()

        return cast(Path, selected)
