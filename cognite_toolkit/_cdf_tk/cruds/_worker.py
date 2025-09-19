import re
import warnings
from collections.abc import Hashable
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, cast

from cognite.client.data_classes import FunctionWrite
from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
)
from cognite.client.data_classes.capabilities import Capability
from rich import print
from rich.panel import Panel
from yaml import YAMLError

from cognite_toolkit._cdf_tk.constants import TABLE_FORMATS
from cognite_toolkit._cdf_tk.exceptions import ToolkitWrongResourceError, ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.tk_warnings import EnvironmentVariableMissingWarning, catch_warnings
from cognite_toolkit._cdf_tk.utils import to_diff

from . import FunctionCRUD
from ._base_cruds import T_ID, ResourceCRUD, T_WritableCogniteResourceList

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.data_classes._module_directories import ReadModule


@dataclass
class CategorizedResources(Generic[T_ID, T_CogniteResourceList]):
    to_create: T_CogniteResourceList
    to_update: T_CogniteResourceList
    to_delete: list[T_ID]
    unchanged: T_CogniteResourceList


class ResourceWorker(
    Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList]
):
    def __init__(
        self,
        loader: ResourceCRUD[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        action: str,
    ):
        self.loader = loader
        self.duplicates: list[T_ID] = []
        self.action = action

    def load_files(
        self, sort: bool = True, directory: Path | None = None, read_modules: "list[ReadModule] | None" = None
    ) -> list[Path]:
        filepaths = self.loader.find_files(directory)

        for read_module in read_modules or []:
            if resource_dir := read_module.resource_dir_path(self.loader.folder_name):
                # As of 05/11/24, Asset support csv and parquet files in addition to YAML.
                # These table formats are not built, i.e., no variable replacement is done,
                # so we load them directly from the source module.
                filepaths.extend(self.loader.find_files(resource_dir, include_formats=TABLE_FORMATS))

        if not sort:
            return filepaths

        def sort_key(p: Path) -> int:
            if result := re.findall(r"^(\d+)", p.stem):
                return int(result[0])
            else:
                return len(filepaths)

        # In the build step, the resource files are prefixed a number that controls the order in which
        # the resources are deployed. The custom 'sort_key' here is to get a sort on integer instead of a default string
        # sort.
        return sorted(filepaths, key=sort_key)

    def prepare_resources(
        self,
        filepaths: list[Path],
        environment_variables: dict[str, str | None] | None = None,
        is_dry_run: bool = False,
        force_update: bool = False,
        verbose: bool = False,
    ) -> CategorizedResources:
        """Prepare resources for deployment by loading them from files, validating access, and categorizing them into create, update, delete, and unchanged lists.

        Args:
            filepaths: The list of file paths to load resources from.
            environment_variables: Environment variables to use for variable replacement in the resource files.
            is_dry_run: Whether to perform a dry run (no actual changes made).
            force_update: Whether to force update existing resources even if they are unchanged.
            verbose: Whether to print detailed information about the resources being processed.

        Returns:
            CategorizedResources: A categorized list of resources to create, update, delete, and unchanged.
        """
        local_by_id = self.load_resources(filepaths, environment_variables, is_dry_run)

        self.validate_access(local_by_id, is_dry_run)

        # Lookup the existing resources in CDF
        cdf_resources: T_WritableCogniteResourceList
        cdf_resources = self.loader.retrieve(list(local_by_id.keys()))
        return self.categorize_resources(local_by_id, cdf_resources, force_update, verbose)

    def load_resources(
        self, filepaths: list[Path], environment_variables: dict[str, str | None] | None, is_dry_run: bool
    ) -> dict[T_ID, tuple[dict[str, Any], T_WriteClass]]:
        local_by_id: dict[T_ID, tuple[dict[str, Any], T_WriteClass]] = {}
        # Load all resources from files, get ids, and remove duplicates.
        environment_variables = environment_variables or {}
        for filepath in filepaths:
            with catch_warnings(EnvironmentVariableMissingWarning) as warning_list:
                try:
                    resource_list = self.loader.load_resource_file(filepath, environment_variables)
                except YAMLError as e:
                    raise ToolkitYAMLFormatError(f"YAML validation error for {filepath.name}: {e}")
            identifiers: list[Hashable] = []
            for resource_dict in resource_list:
                identifier = self.loader.get_id(resource_dict)
                identifiers.append(identifier)
                try:
                    # The load resource modifies the resource_dict, so we deepcopy it to avoid side effects.
                    loaded = self.loader.load_resource(deepcopy(resource_dict), is_dry_run)
                except ToolkitWrongResourceError:
                    # The ToolkitWrongResourceError is a special exception that as of 21/12/24 is used by
                    # the GroupAllScopedLoader and GroupResourceScopedLoader to signal that the resource
                    # should be handled by the other loader.
                    continue
                if identifier in local_by_id:
                    self.duplicates.append(identifier)
                else:
                    local_by_id[identifier] = resource_dict, loaded

            for warning in warning_list:
                if isinstance(warning, EnvironmentVariableMissingWarning):
                    # Warnings are immutable, so we use the below method to override it.
                    object.__setattr__(warning, "identifiers", frozenset(identifiers))
                    # Reraise the warning to be caught higher up.
                    warnings.warn(warning, stacklevel=2)
                else:
                    warning.print_warning()
        return local_by_id

    def validate_access(self, local_by_id: dict[T_ID, tuple[dict[str, Any], T_WriteClass]], is_dry_run: bool) -> None:
        capabilities: Capability | list[Capability]
        if isinstance(self.loader, FunctionCRUD):
            function_loader: FunctionCRUD = self.loader
            function_items = cast(list[FunctionWrite], [item for _, item in local_by_id.values()])
            capabilities = function_loader.get_function_required_capabilities(function_items, read_only=is_dry_run)
        else:
            capabilities = self.loader.get_required_capability(
                [item for _, item in local_by_id.values()], read_only=is_dry_run
            )
        if capabilities and (missing := self.loader.client.verify.authorization(capabilities)):
            raise self.loader.client.verify.create_error(missing, action=f"{self.action} {self.loader.display_name}")

    def categorize_resources(
        self,
        local_by_id: dict[T_ID, tuple[dict[str, Any], T_WriteClass]],
        cdf_resources: T_WritableCogniteResourceList,
        force_update: bool,
        verbose: bool,
    ) -> CategorizedResources:
        resources: CategorizedResources[T_ID, T_CogniteResourceList] = CategorizedResources(
            to_create=self.loader.list_write_cls([]),
            to_update=self.loader.list_write_cls([]),
            to_delete=[],
            unchanged=self.loader.list_write_cls([]),
        )
        cdf_resource_by_id = {self.loader.get_id(resource): resource for resource in cdf_resources}
        for identifier, (local_dict, local_resource) in local_by_id.items():
            cdf_resource = cdf_resource_by_id.get(identifier)
            if cdf_resource is None:
                resources.to_create.append(local_resource)
                continue
            cdf_dict = self.loader.dump_resource(cdf_resource, local_dict)
            if not force_update and cdf_dict == local_dict:
                resources.unchanged.append(local_resource)
                continue
            if self.loader.support_update:
                resources.to_update.append(local_resource)
            else:
                resources.to_delete.append(identifier)
                resources.to_create.append(local_resource)
            if verbose:
                diff_str = "\n".join(to_diff(cdf_dict, local_dict))
                for sensitive in self.loader.sensitive_strings(local_resource):
                    diff_str = diff_str.replace(sensitive, "********")
                print(
                    Panel(
                        diff_str,
                        title=f"{self.loader.display_name}: {identifier}",
                        expand=False,
                    )
                )
        return resources
