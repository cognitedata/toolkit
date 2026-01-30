import difflib
from pathlib import Path
from typing import cast

import questionary
import typer
from questionary import Choice
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import RESOURCE_CRUD_LIST, ResourceCRUD
from cognite_toolkit._cdf_tk.data_classes import ModuleDirectories
from cognite_toolkit._cdf_tk.resource_classes import ToolkitResource
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump


class ResourcesCommand(ToolkitCommand):
    def _get_or_prompt_module_path(self, module: str | None, organization_dir: Path, verbose: bool) -> Path:
        """
        Check if the module exists in the organization directory and return the module path.
        If module is not provided, ask the user to select or create a new module.
        """
        present_modules = ModuleDirectories.load(organization_dir, None)

        if module:
            for mod in present_modules:
                if mod.name.casefold() == module.casefold():
                    return mod.dir

            if questionary.confirm(f"{module} module not found. Do you want to create a new one?").unsafe_ask():
                return organization_dir / MODULES / module

            if verbose:
                print(f"[red]Aborting as {module} module not found...[/red]")
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
            return organization_dir / MODULES / new_module_name

        if not selected:
            print("[red]No module selected. Aborting...[/red]")
            raise typer.Exit()

        return cast(Path, selected)

    def _resolve_kinds(self, kinds: list[str] | None) -> list[type[ResourceCRUD]]:
        """
        Resolve kinds from list of strings or do an interactive selection.
        """
        all_cruds = {crud.kind.casefold(): crud for crud in RESOURCE_CRUD_LIST}

        if not kinds:
            sorted_cruds = sorted(RESOURCE_CRUD_LIST, key=lambda x: x.kind)
            choices = [Choice(title=crud.kind, value=crud) for crud in sorted_cruds]

            selected = questionary.select("Select resource type:", choices=choices).unsafe_ask()
            if not selected:
                print("[red]No resource type selected. Aborting...[/red]")
                raise typer.Exit()
            return [selected]

        resolved_cruds = []
        for kind in kinds:
            kind_lower = kind.casefold()
            if kind_lower in all_cruds:
                resolved_cruds.append(all_cruds[kind_lower])
            else:
                matches = difflib.get_close_matches(kind_lower, all_cruds.keys())
                if matches:
                    suggestion = all_cruds[matches[0]].kind
                    print(f"[red]Unknown resource type '{kind}'. Did you mean '{suggestion}'?[/red]")
                else:
                    print(
                        f"[red]Unknown resource type '{kind}'. "
                        f"Available types: {humanize_collection(sorted([c.kind for c in RESOURCE_CRUD_LIST]))}[/red]"
                    )
                raise typer.Exit()

        return resolved_cruds

    def _create_resource_yaml_skeleton(self, yaml_cls: type[ToolkitResource]) -> dict[str, str]:
        """
        Build YAML skeleton from a Pydantic model class using JSON schema for better type information.
        """
        yaml_skeleton: dict[str, str] = {}
        for field_name, field in yaml_cls.model_fields.items():
            name = field.alias or field_name
            description = field.description or name
            if field.is_required():
                yaml_skeleton[name] = f"(Required) {description}"
            else:
                yaml_skeleton[name] = description

        return yaml_skeleton

    def _get_resource_yaml_content(self, resource_crud: type[ResourceCRUD]) -> str:
        """
        Creates a new resource in the specified module using the resource_crud.yaml_cls.
        """
        yaml_header = (
            f"# API docs: {resource_crud.doc_url()}\n"
            f"# YAML reference: https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/resource_library"
        )
        yaml_skeleton = self._create_resource_yaml_skeleton(resource_crud.yaml_cls)
        yaml_contents = yaml_safe_dump(yaml_skeleton)
        return yaml_header + "\n\n" + yaml_contents

    def _create_resource_yaml_file(
        self,
        resource_crud: type[ResourceCRUD],
        module_path: Path,
        prefix: str | None = None,
        verbose: bool = False,
    ) -> None:
        """
        Creates a new resource YAML file in the specified module using the resource_crud.yaml_cls.
        """
        resource_dir: Path = module_path / resource_crud.folder_name
        if resource_crud.sub_folder_name:
            resource_dir = resource_dir / resource_crud.sub_folder_name

        if not resource_dir.exists():
            resource_dir.mkdir(parents=True, exist_ok=True)

        final_prefix = prefix if prefix is not None else f"my_{resource_crud.kind}"
        file_name = f"{final_prefix}.{resource_crud.kind}.yaml"
        file_path: Path = resource_dir / file_name

        if (
            file_path.exists()
            and not questionary.confirm(f"{file_path.name} file already exists. Overwrite?").unsafe_ask()
        ):
            print("[red]Skipping...[/red]")
            return

        yaml_content = self._get_resource_yaml_content(resource_crud)
        file_path.write_text(yaml_content)
        if verbose:
            print(
                f"[green]{resource_crud.kind} Resource YAML file created successfully at {file_path.as_posix()}[/green]"
            )
        else:
            print(f"[green]Created {file_path.as_posix()}[/green]")

    def create(
        self,
        organization_dir: Path,
        module_name: str | None = None,
        kind: list[str] | None = None,
        prefix: str | None = None,
        verbose: bool = False,
    ) -> None:
        """
        create resource YAMLs.

        Args:
            organization_dir: The directory of the organization.
            module_name: The name of the module.
            kind: The kind(s) of resource to create.
            prefix: The prefix for the resource file.
            verbose: Whether to print verbose output.
        """
        module_path = self._get_or_prompt_module_path(module_name, organization_dir, verbose)
        resource_cruds = self._resolve_kinds(kind)
        for crud in resource_cruds:
            self._create_resource_yaml_file(crud, module_path, prefix, verbose)
