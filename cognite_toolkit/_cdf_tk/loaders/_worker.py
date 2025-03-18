from __future__ import annotations

import re
import warnings
from collections.abc import Hashable
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, Literal, overload

from cognite.client.data_classes._base import (
    T_CogniteResourceList,
    T_WritableCogniteResource,
    T_WriteClass,
)
from rich import print
from rich.panel import Panel
from yaml import YAMLError

from cognite_toolkit._cdf_tk.constants import TABLE_FORMATS
from cognite_toolkit._cdf_tk.exceptions import ToolkitWrongResourceError, ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.tk_warnings import EnvironmentVariableMissingWarning, catch_warnings
from cognite_toolkit._cdf_tk.utils import to_diff

from ._base_loaders import T_ID, ResourceLoader, T_WritableCogniteResourceList

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.data_classes._module_directories import ReadModule


class ResourceWorker(
    Generic[T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList]
):
    def __init__(
        self,
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
    ):
        self.loader = loader

    def load_files(
        self, sort: bool = True, directory: Path | None = None, read_modules: list[ReadModule] | None = None
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

    @overload
    def load_resources(
        self,
        filepaths: list[Path],
        return_existing: Literal[True],
        environment_variables: dict[str, str | None] | None = None,
        is_dry_run: bool = False,
        force_update: bool = False,
        verbose: bool = False,
    ) -> tuple[T_WritableCogniteResourceList, list[T_ID]]: ...

    @overload
    def load_resources(
        self,
        filepaths: list[Path],
        return_existing: Literal[False] = False,
        environment_variables: dict[str, str | None] | None = None,
        is_dry_run: bool = False,
        force_update: bool = False,
        verbose: bool = False,
    ) -> tuple[T_CogniteResourceList, T_CogniteResourceList, list[T_ID], T_CogniteResourceList, list[T_ID]]: ...

    def load_resources(
        self,
        filepaths: list[Path],
        return_existing: bool = False,
        environment_variables: dict[str, str | None] | None = None,
        is_dry_run: bool = False,
        force_update: bool = False,
        verbose: bool = False,
    ) -> (
        tuple[T_CogniteResourceList, T_CogniteResourceList, list[T_ID], T_CogniteResourceList, list[T_ID]]
        | tuple[T_WritableCogniteResourceList, list[T_ID]]
    ):
        duplicates: list[T_ID] = []
        local_by_id: dict[T_ID, tuple[dict[str, Any], T_WriteClass]] = {}  # type: ignore[assignment]
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
                    duplicates.append(identifier)
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

        capabilities = self.loader.get_required_capability(
            [item for _, item in local_by_id.values()], read_only=is_dry_run
        )
        if capabilities and (missing := self.loader.client.verify.authorization(capabilities)):
            raise self.loader.client.verify.create_error(missing, action=f"clean {self.loader.display_name}")

        # Lookup the existing resources in CDF
        cdf_resources: T_WritableCogniteResourceList
        cdf_resources = self.loader.retrieve(list(local_by_id.keys()))
        if return_existing:
            return cdf_resources, duplicates

        to_create: T_CogniteResourceList
        to_update: T_CogniteResourceList
        to_delete: list[T_ID] = []
        unchanged: T_CogniteResourceList
        to_create, to_update, unchanged = (
            self.loader.list_write_cls([]),
            self.loader.list_write_cls([]),
            self.loader.list_write_cls([]),
        )
        cdf_resource_by_id = {self.loader.get_id(resource): resource for resource in cdf_resources}
        for identifier, (local_dict, local_resource) in local_by_id.items():
            cdf_resource = cdf_resource_by_id.get(identifier)
            if cdf_resource is None:
                to_create.append(local_resource)
                continue
            cdf_dict = self.loader.dump_resource(cdf_resource, local_dict)
            if not force_update and cdf_dict == local_dict:
                unchanged.append(local_resource)
                continue
            if self.loader.support_update:
                to_update.append(local_resource)
            else:
                to_delete.append(identifier)
                to_create.append(local_resource)
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
        return to_create, to_update, to_delete, unchanged, duplicates
