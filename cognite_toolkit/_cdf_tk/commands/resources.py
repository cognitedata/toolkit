from pathlib import Path

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand


class ResourcesCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning, skip_tracking, silent)

    def create(
        self,
        organization_dir: Path,
        module_name: str,
        resource_directory: str,
        resources: list[str] | None = None,
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
        print(organization_dir, module_name, resource_directory, resources, file_name, verbose)

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
