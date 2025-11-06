import difflib
from pathlib import Path

import questionary
import typer
from rich import print

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME, ContainerCRUD, NodeCRUD, ResourceCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.data_classes import ModuleDirectories
from cognite_toolkit._cdf_tk.resource_classes import ToolkitResource
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump


class ResourcesCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)

    def _get_module_path(self, module: str, organization_dir: Path, verbose: bool) -> Path:
        """
        check if the module exists in the organization directory and return the module path.
        if not, ask the user if they want to create a new module.
        """
        present_modules = ModuleDirectories.load(organization_dir, None)

        for mod in present_modules:
            if mod.name.casefold() == module.casefold():
                return mod.dir

        if questionary.confirm(f"{module} module not found. Do you want to create a new one?").ask():
            return organization_dir / MODULES / module

        if verbose:
            print(f"[red]Aborting as {module} module not found and user did not want to create a new one...[/red]")
        else:
            print("[red]Aborting...[/red]")
        raise typer.Exit()

    def _create_default_file_name(self, resource_crud: type[ResourceCRUD]) -> str:
        """
        Helper function to return the default file name for a resource.
        """
        return f"my_{resource_crud.kind.strip().replace(' ', '_')}"

    def _get_resource_cruds(
        self,
        resource_directories: str | list[str],
        resources: list[str],
        verbose: bool,
        file_name: list[str] | None = None,
    ) -> list[tuple[type[ResourceCRUD], str]]:
        """
        get the resource cruds for the resource directory.
        """
        if isinstance(resource_directories, str):
            resource_directories = [resource_directories]

        folder_names: list[str] = list(CRUDS_BY_FOLDER_NAME.keys())
        for rd in resource_directories:
            if rd not in folder_names:
                if verbose and (close_matches := difflib.get_close_matches(rd, folder_names)):
                    print(
                        f"[red]{rd} is an invalid resource directory. Did you mean one of the following: {close_matches}?[/red]"
                    )
                else:
                    print(f"[red]{rd} is an invalid resource directory.[/red]")
                raise typer.Exit()
        file_name_map: dict[str, str] = {}
        if file_name:
            file_name_map = dict(zip(resources, file_name))

        resource_cruds: list[tuple[type[ResourceCRUD], str]] = []
        for _dir in resource_directories:
            resource_cruds.extend(
                [
                    (crud, file_name_map.get(crud.kind, self._create_default_file_name(crud)))
                    for crud in CRUDS_BY_FOLDER_NAME[_dir]
                    if isinstance(crud, type) and issubclass(crud, ResourceCRUD) and crud.kind in resources
                ]
            )
        return resource_cruds

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

    def _get_sub_directory_name(self, resource_crud: type[ResourceCRUD]) -> str | None:
        """
        Checks if the sub folder is present in the module path.
        """
        resources_subfolder_mapping = {
            ContainerCRUD: "containers",
            NodeCRUD: "nodes",
            ViewCRUD: "views",
        }
        return resources_subfolder_mapping.get(resource_crud, None)

    def _create_resource_yaml_file(
        self,
        resource_crud: type[ResourceCRUD],
        file_name: str,
        module_path: Path,
        verbose: bool,
    ) -> None:
        """
        Creates a new resource YAML file in the specified module using the resource_crud.yaml_cls.
        """
        resource_dir: Path = module_path / resource_crud.folder_name
        if sub_directory := self._get_sub_directory_name(resource_crud):
            resource_dir = resource_dir / sub_directory

        if not resource_dir.exists():
            resource_dir.mkdir(parents=True, exist_ok=True)

        file_path: Path = resource_dir / f"{file_name}.{resource_crud.kind}.yaml"

        if file_path.exists() and not questionary.confirm(f"{file_path.name} file already exists. Overwrite?").ask():
            print("[red]Skipping...[/red]")
            return

        yaml_content = self._get_resource_yaml_content(resource_crud)
        file_path.write_text(yaml_content)
        if verbose:
            print(
                f"[green]{resource_crud.kind} Resource YAML file created successfully at {file_path.as_posix()}[/green]"
            )
        else:
            print(f"[green]Created {file_path.name}[/green]")

    def create(
        self,
        organization_dir: Path,
        module_name: str,
        resource_directory: str,
        resources: list[str],
        file_name: list[str] | None = None,
        verbose: bool = False,
    ) -> None:
        """
        create resource YAMLs using CLI arguments.

        Args:
            organization_dir: The directory of the organization.
            module_name: The name of the module.
            resource_directory: The resource directory to create the resources in.
            resources: The resources to create under the resource directory.
            file_name: The name of the resource file to create.
            verbose: Whether to print verbose output.
        """
        module_path: Path = self._get_module_path(module_name, organization_dir, verbose)
        resource_cruds: list[tuple[type[ResourceCRUD], str]] = self._get_resource_cruds(
            resource_directory, resources, verbose, file_name
        )
        for resource_crud, file_name_str in resource_cruds:
            self._create_resource_yaml_file(resource_crud, file_name_str, module_path, verbose)

    def create_interactive(
        self,
        organization_dir: Path,
        module_name: str,
        resource_directories: list[str] | None = None,
        verbose: bool = False,
    ) -> None:
        """
        create resource YAMLs using interactive prompts.

        Args:
            organization_dir: The directory of the organization.
            module_name: The name of the module.
            resource_directories: The resource directories to create the resources in.
            verbose: Whether to print verbose output.
        """
        print(organization_dir, module_name, resource_directories, verbose)
