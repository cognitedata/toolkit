import difflib
import subprocess
from pathlib import Path
from typing import Any, cast

import questionary
import typer
from questionary import Choice
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.functions import ScaffoldDef
from cognite_toolkit._cdf_tk.commands.functions import get_scaffolds as _fn_scaffolds
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.data_classes import ModuleDirectories
from cognite_toolkit._cdf_tk.resources_ios import RESOURCE_CRUD_LIST, ResourceIO
from cognite_toolkit._cdf_tk.utils.collection import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import validate_safe_path, yaml_safe_dump

# Scaffold variants keyed by CRUD kind (casefold). Each entry is a list of
# ScaffoldDef variants the user can choose from after the YAML is created.
# To add scaffolds for a new resource type, add a get_scaffolds() in its
# command module and merge it here.
_ALL_SCAFFOLDS: dict[str, list[ScaffoldDef]] = {**_fn_scaffolds()}


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
                validate_safe_path(module)
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
            validate_safe_path(new_module_name)
            return organization_dir / MODULES / new_module_name

        if not selected:
            print("[red]No module selected. Aborting...[/red]")
            raise typer.Exit()

        return cast(Path, selected)

    @staticmethod
    def _qualified_name(crud: type[ResourceIO]) -> str:
        return f"{crud.folder_name}.{crud.kind}"

    @classmethod
    def _unique_cruds(cls) -> list[type[ResourceIO]]:
        """Deduplicate and sort RESOURCE_CRUD_LIST by folder_name.kind."""
        return sorted(
            {(c.folder_name, c.kind): c for c in RESOURCE_CRUD_LIST}.values(),
            key=cls._qualified_name,
        )

    def _resolve_kinds(self, kinds: list[str] | None) -> list[type[ResourceIO]]:
        """
        Resolve kinds from list of strings or do an interactive selection.
        Accepts both plain kind names (e.g. 'Space') and qualified names (e.g. 'data_modeling.Space').
        """
        unique_cruds = self._unique_cruds()

        if not kinds:
            choices = [Choice(title=self._qualified_name(crud), value=crud) for crud in unique_cruds]

            selected = questionary.select("Select resource type:", choices=choices).unsafe_ask()
            if not selected:
                print("[red]No resource type selected. Aborting...[/red]")
                raise typer.Exit()
            return [selected]

        by_qualified = {self._qualified_name(crud).casefold(): crud for crud in unique_cruds}
        by_kind: dict[str, list[type[ResourceIO]]] = {}
        for crud in unique_cruds:
            by_kind.setdefault(crud.kind.casefold(), []).append(crud)

        resolved_cruds = []
        for kind in kinds:
            kind_lower = kind.casefold()

            if kind_lower in by_qualified:
                resolved_cruds.append(by_qualified[kind_lower])
                continue

            if kind_lower in by_kind:
                matches = by_kind[kind_lower]
                if len(matches) == 1:
                    resolved_cruds.append(matches[0])
                    continue
                qualified_names = sorted(self._qualified_name(m) for m in matches)
                print(
                    f"[red]Ambiguous resource type '{kind}'. "
                    f"Did you mean: {humanize_collection(qualified_names)}?[/red]"
                )
                raise typer.Exit()

            all_names = list(by_qualified.keys()) + [k for k, v in by_kind.items() if len(v) == 1]
            close = difflib.get_close_matches(kind_lower, all_names)
            if close:
                match = close[0]
                if match in by_qualified:
                    suggestion = self._qualified_name(by_qualified[match])
                else:
                    suggestion = by_kind[match][0].kind
                print(f"[red]Unknown resource type '{kind}'. Did you mean '{suggestion}'?[/red]")
            else:
                available = sorted(self._qualified_name(c) for c in unique_cruds)
                print(f"[red]Unknown resource type '{kind}'. Available types: {humanize_collection(available)}[/red]")
            raise typer.Exit()

        return resolved_cruds

    def _get_resource_yaml_content(
        self,
        resource_crud: type[ResourceIO],
        overrides: dict[str, Any] | None = None,
    ) -> str:
        """
        Creates a new resource in the specified module using the resource_crud.yaml_cls.
        Each field is rendered with its default value and a comment describing it.

        Args:
            resource_crud: The CRUD class whose yaml_cls defines the fields.
            overrides: Optional dict of field name → value to substitute into the
                generated YAML instead of the placeholder/default.
        """
        overrides = overrides or {}
        lines = [
            f"# API docs: {resource_crud.doc_url()}",
            "# YAML reference: https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/resource_library",
            "",
        ]

        required_fields: list[tuple[str, Any, str]] = []
        optional_with_value: list[tuple[str, Any, str]] = []
        optional_null: list[tuple[str, None, str]] = []

        for field_name, field in resource_crud.yaml_cls.model_fields.items():
            name = field.alias or field_name
            description = field.description or ""

            if name in overrides:
                # Overridden fields are always emitted with their value (no placeholder).
                required_fields.append((name, overrides[name], f"# {description}"))
            elif field.is_required():
                required_fields.append((name, f"<{name}>", f"# (Required) {description}"))
            elif field.default_factory is not None:
                # Will fail for factories that require validated data as input (one-arg variant).
                value = field.default_factory()  # type: ignore[call-arg]
                optional_with_value.append((name, value, f"# {description}"))
            elif field.default is not None:
                optional_with_value.append((name, field.default, f"# {description}"))
            else:
                optional_null.append((name, None, f"# {description}"))

        for group in (required_fields, optional_with_value):
            for name, value, comment in group:
                yaml_block = yaml_safe_dump({name: value}, sort_keys=False).rstrip("\n")
                # Add comment to the first line of the YAML block
                block_lines = yaml_block.split("\n")
                block_lines[0] = f"{block_lines[0]}  {comment}"
                lines.append("\n".join(block_lines))

        if optional_null:
            lines.append("")
            lines.append("# Optional fields (uncomment to use):")
            for name, _, comment in optional_null:
                lines.append(f"# {name}:  {comment}")

        return "\n".join(lines) + "\n"

    def _create_resource_yaml_file(
        self,
        resource_crud: type[ResourceIO],
        module_path: Path,
        prefix: str | None = None,
        verbose: bool = False,
        prompt_external_id: bool = False,
    ) -> str:
        """
        Creates a new resource YAML file in the specified module using the resource_crud.yaml_cls.
        Returns the prefix used (which doubles as the external_id for follow-up scaffold steps).
        """
        resource_dir: Path = module_path / resource_crud.folder_name
        if resource_crud.sub_folder_name:
            resource_dir = resource_dir / resource_crud.sub_folder_name

        if not resource_dir.exists():
            resource_dir.mkdir(parents=True, exist_ok=True)

        if prefix is not None:
            final_prefix = prefix
        elif prompt_external_id:
            final_prefix = questionary.text(
                f"Enter an externalId for the {resource_crud.kind}:",
                default=f"my_{resource_crud.kind}",
            ).unsafe_ask()
        else:
            final_prefix = f"my_{resource_crud.kind}"
        validate_safe_path(final_prefix)
        file_name = f"{final_prefix}.{resource_crud.kind}.yaml"
        file_path: Path = resource_dir / file_name

        if (
            file_path.exists()
            and not questionary.confirm(f"{file_path.name} file already exists. Overwrite?").unsafe_ask()
        ):
            print("[red]Skipping...[/red]")
            return final_prefix

        overrides: dict[str, Any] = {"externalId": final_prefix}
        if "name" in resource_crud.yaml_cls.model_fields:
            overrides["name"] = final_prefix
        owner = self._get_git_user()
        if owner and "owner" in resource_crud.yaml_cls.model_fields:
            overrides["owner"] = owner
        yaml_content = self._get_resource_yaml_content(resource_crud, overrides=overrides)
        file_path.write_text(yaml_content)
        if verbose:
            print(
                f"[green]{resource_crud.kind} Resource YAML file created successfully at {file_path.as_posix()}[/green]"
            )
        else:
            print(f"[green]Created {file_path.as_posix()}[/green]")

        return final_prefix

    @staticmethod
    def _get_git_user() -> str | None:
        """Return 'Name <email>' from git config, or None if unavailable."""
        try:
            name = subprocess.check_output(["git", "config", "user.name"], text=True).strip()
            email = subprocess.check_output(["git", "config", "user.email"], text=True).strip()
        except (subprocess.CalledProcessError, FileNotFoundError):
            return None
        if name and email:
            return f"{name} <{email}>"
        return name or None

    @staticmethod
    def _pick_scaffold(variants: list[ScaffoldDef]) -> ScaffoldDef | None:
        """Prompt the user to pick a scaffold variant, or return the only one."""
        if len(variants) == 1:
            return variants[0]
        choices = [Choice(title=f"{v.label}  — {v.description}", value=v) for v in variants]
        return questionary.select("Select a variant:", choices=choices, default=choices[0]).unsafe_ask()

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

        for crud in self._resolve_kinds(kind):
            variants = _ALL_SCAFFOLDS.get(crud.kind.casefold())
            scaffold = self._pick_scaffold(variants) if variants else None
            external_id = self._create_resource_yaml_file(
                crud,
                module_path,
                prefix,
                verbose,
                prompt_external_id=scaffold is not None,
            )
            if scaffold:
                scaffold.run(module_path, external_id, self)
