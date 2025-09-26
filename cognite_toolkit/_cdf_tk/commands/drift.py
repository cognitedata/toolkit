from __future__ import annotations

import difflib
import shutil
import tempfile
from pathlib import Path
from typing import Any

import questionary
from questionary import Choice
from rich import print
from rich.table import Table

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import BuildCommand
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.transformation import TransformationCRUD
from cognite_toolkit._cdf_tk.data_classes import BuiltFullResourceList, BuiltModuleList, ModuleDirectories
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.file import safe_read, safe_write, yaml_safe_dump


class DriftCommand(ToolkitCommand):
    def run_transformations(
        self,
        env_vars: EnvironmentVariables,
        organization_dir: Path,
        env_name: str | None,
        dry_run: bool,
        yes: bool,
        verbose: bool,
    ) -> None:
        client: ToolkitClient = env_vars.get_client()
        build_dir = Path(tempfile.mkdtemp())
        try:
            built_modules: BuiltModuleList = BuildCommand(silent=True, skip_tracking=True).execute(
                verbose=verbose,
                organization_dir=organization_dir,
                build_dir=build_dir,
                selected=None,
                build_env_name=env_name,
                no_clean=False,
                client=client,
                on_error="raise",
            )

            # Collect local transformations as built resources
            loader: TransformationCRUD = TransformationCRUD(client, build_dir=None)
            local_full: BuiltFullResourceList[str] = built_modules.get_resources(
                id_type=str,
                resource_dir=loader.folder_name,  # type: ignore[arg-type]
                kind=loader.kind,
            )
            env_map: dict[str, str | None] = {}
            local_dict_by_id = self._get_local_dict_by_id(local_full, loader, env_map)

            ui_list = client.transformations.list(limit=-1)
            ui_by_id = {t.external_id: t for t in ui_list if getattr(t, "external_id", None)}
            local_only, ui_only, both = self._bucketize(set(list(local_dict_by_id.keys())), set(list(ui_by_id.keys())))  # type: ignore[arg-type]

            self._display_drift_table(local_only, ui_only, both)

            if local_only:
                self._handle_local_only(
                    local_only_ids=local_only,
                    env_vars=env_vars,
                    build_dir=build_dir,
                    env_name=env_name,
                    dry_run=dry_run,
                    yes=yes,
                    verbose=verbose,
                )

            if ui_only and questionary.confirm("Do you want to dump transformations in CDF into a module?").ask():
                self._handle_ui_only(ui_only, ui_by_id, organization_dir, client, verbose, dry_run, yes)  # type: ignore[arg-type]

            if (
                both
                and questionary.confirm("Do you want to show diffs for transformations in both local and CDF?").ask()
            ):
                self._handle_both_present(
                    both_ids=both,
                    local_full=local_full,
                    local_dict_by_id=local_dict_by_id,
                    ui_by_id=ui_by_id,  # type: ignore[arg-type]
                    loader=loader,
                    env_map=env_map,
                    dry_run=dry_run,
                    yes=yes,
                    verbose=verbose,
                )
        finally:
            print("[bold green] CDF and local resources are synced![/]")

    @staticmethod
    def _get_local_dict_by_id(
        resources: BuiltFullResourceList[str], loader: ResourceCRUD, environment_variables: dict[str, str | None]
    ) -> dict[str, dict[str, Any]]:
        # Mirrors PullCommand._get_local_resource_dict_by_id
        unique_destinations = {r.destination for r in resources if r.destination}
        local_by_id: dict[str, dict[str, Any]] = {}
        local_ids = set(resources.identifiers)
        for destination in unique_destinations:
            if not destination:
                continue
            for resource_dict in loader.load_resource_file(destination, environment_variables):
                identifier = loader.get_id(resource_dict)
                if identifier in local_ids:
                    local_by_id[identifier] = resource_dict
        return local_by_id

    @staticmethod
    def _bucketize(local_ids: set[str], ui_ids: set[str]) -> tuple[list[str], list[str], list[str]]:
        local_only = sorted(local_ids - ui_ids)
        ui_only = sorted(ui_ids - local_ids)
        both = sorted(local_ids & ui_ids)
        return local_only, ui_only, both

    def _display_drift_table(self, local_only: list[str], ui_only: list[str], both: list[str]) -> None:
        table = Table(title="Transformation Drift Summary", show_header=True, header_style="bold magenta")
        table.add_column("Category", style="bold", justify="center")
        table.add_column("Count", style="cyan", justify="center")
        table.add_column("External IDs", justify="center")

        def format_ids(ids: list[str]) -> str:
            return "\n".join(ids) if ids else "[dim]None[/dim]"

        table.add_row("Local only", str(len(local_only)), format_ids(local_only))
        table.add_row("CDF only", str(len(ui_only)), format_ids(ui_only))
        table.add_row("Common in both", str(len(both)), format_ids(both))
        print(table)

    def _handle_ui_only(
        self,
        ui_only_ids: list[str],
        ui_by_id: dict[str, Any],
        organization_dir: Path,
        client: ToolkitClient,
        verbose: bool,
        dry_run: bool,
        yes: bool,
    ) -> None:
        modules = ModuleDirectories.load(organization_dir, None)
        if dry_run:
            print(f"[bold]UI-only[/]: {len(ui_only_ids)} (dry-run, no writes)")
            return
        if yes:
            # Auto-pick first available module or create a default one
            if modules:
                target_dir: Path = modules[0].dir
            else:
                target_dir = organization_dir / "drift_imports"
                (target_dir / "transformations").mkdir(parents=True, exist_ok=True)
        else:
            choices = [Choice(title=m.name, value=m.dir) for m in modules]
            choices.append(Choice(title="Create new module", value="__create_new__"))
            selected = questionary.select("Select a module to save UI-only transformations", choices=choices).ask()
            if not selected:
                print("[yellow]No module selected. Skipping UI-only dump.[/]")
                return
            if selected == "__create_new__":
                new_name = questionary.text("Enter new module name").ask()
                if not new_name:
                    print("[yellow]No module created. Skipping UI-only dump.[/]")
                    return
                target_dir: Path = organization_dir / "modules" / new_name  # type: ignore[no-redef]
                (target_dir / "transformations").mkdir(parents=True, exist_ok=True)
            else:
                target_dir = selected

        loader: TransformationCRUD = TransformationCRUD(client, build_dir=None)
        for tid in ui_only_ids:
            resource = ui_by_id[tid]
            dumped = loader.dump_resource(resource)
            name = loader.as_str(tid)
            base_filepath = Path(target_dir) / loader.folder_name / f"{name}.{loader.kind}.yaml"
            base_filepath.parent.mkdir(parents=True, exist_ok=True)
            for filepath, subpart in loader.split_resource(base_filepath, dumped):
                content = subpart if isinstance(subpart, str) else yaml_safe_dump(subpart)
                safe_write(filepath, content, encoding="utf-8")
            if verbose:
                print(
                    f"Synchronized [green]{loader.kind}[/green] [green]{name}[/green] to [green]{base_filepath.parent}[/green]"
                )

    def _handle_both_present(
        self,
        both_ids: list[str],
        local_full: BuiltFullResourceList[str],
        local_dict_by_id: dict[str, dict[str, Any]],
        ui_by_id: dict[str, Any],
        loader: ResourceCRUD,
        env_map: dict[str, str | None],
        dry_run: bool,
        yes: bool,
        verbose: bool,
    ) -> None:
        # Map identifier -> built resource for quick lookup
        full_by_id = {r.identifier: r for r in local_full}
        for rid in both_ids:
            built = full_by_id[rid]
            local_dict = local_dict_by_id[rid]
            cdf_dumped = loader.dump_resource(ui_by_id[rid], local_dict)
            if cdf_dumped == local_dict:
                print(f"[bold yellow]No change in [green]{rid}[/green][/]")
                continue
            # Show YAML diff
            print(f"[bold yellow]Displaying diff for [green]{rid}[/green][/]")
            build_content = safe_read(built.source.path)
            updated_yaml = yaml_safe_dump(cdf_dumped)

            self._display_diff(build_content, updated_yaml)

            choice = (
                "local"
                if yes
                else questionary.select(
                    f"Keep local or overwrite with CDF for {rid}?",
                    choices=[Choice("Keep local", value="local"), Choice("Use CDF", value="cdf")],
                ).ask()
            )
            if choice == "local" or dry_run:
                continue
            # Backup and overwrite
            dest = built.source.path
            backup = dest.with_suffix(dest.suffix + ".bak")
            try:
                shutil.copy2(dest, backup)
            except Exception:
                pass
            safe_write(dest, updated_yaml, encoding="utf-8")
            # Remove backup after successful writing
            try:
                backup.unlink()
            except Exception:
                pass
            if verbose:
                print(f"[bold green]Updated [green]{rid}[/green] in local[/]")

    def _display_diff(self, build_content: str, updated_yaml: str) -> None:
        lines1 = build_content.splitlines()
        lines2 = updated_yaml.splitlines()
        # diff_lines = difflib.unified_

        differ = difflib.Differ()
        diff = list(differ.compare(lines1, lines2))

        print(f"{'--- YAML in Local ---':<50} | {'+++ YAML in CDF ---':<50}")
        print("-" * 105)

        line_num1, line_num2 = 0, 0
        # Process the line-by-line diff to display in two columns
        for line in diff:
            code = line[:2]
            text = line[2:]

            # Lines unique to file 1 or changed
            if code == "- ":
                line_num1 += 1
                print(f"{line_num1:03d} {text:<46} | {'':<50}")
            # Lines unique to file 2 or changed
            elif code == "+ ":
                line_num2 += 1
                print(f"{'':<50} | {line_num2:03d} {text:<46}")
            # Lines common to both
            elif code == "  ":
                line_num1 += 1
                line_num2 += 1
                print(f"{line_num1:03d} {text:<46} | {line_num2:03d} {text:<46}")
            # Ignore other metadata from difflib
            else:
                continue

    def _handle_local_only(
        self,
        local_only_ids: list[str],
        env_vars: EnvironmentVariables,
        build_dir: Path,
        env_name: str | None,
        dry_run: bool,
        yes: bool,
        verbose: bool,
    ) -> None:
        print("[yellow]Use `cdf build` and `cdf deploy` to deploy local-only transformations to UI.[/]")
