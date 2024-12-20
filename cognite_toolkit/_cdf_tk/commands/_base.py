from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from cognite.client.exceptions import CogniteAPIError
from rich import print
from rich.panel import Panel
from yaml import YAMLError

from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError, ToolkitTypeError, ToolkitYAMLFormatError
from cognite_toolkit._cdf_tk.loaders import (
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.tk_warnings import (
    LowSeverityWarning,
    ToolkitWarning,
    WarningList, MediumSeverityWarning,
)
from cognite_toolkit._cdf_tk.tracker import Tracker
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig, to_diff,
)

_HAS_PRINTED_COLLECT_MESSAGE = False


class ToolkitCommand:
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False):
        self._print_warning = print_warning
        self.silent = silent
        self.warning_list = WarningList[ToolkitWarning]()
        self.tracker = Tracker(skip_tracking)

    @property
    def print_warning(self) -> bool:
        return self._print_warning and not self.silent

    def _track_command(self, result: str | Exception) -> None:
        self.tracker.track_cli_command(self.warning_list, result, type(self).__name__.removesuffix("Command"))

    def run(self, execute: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        global _HAS_PRINTED_COLLECT_MESSAGE
        is_collect_command = len(sys.argv) >= 2 and "collect" == sys.argv[1]
        if (
            not self.tracker.opted_in
            and not self.tracker.opted_out
            and not is_collect_command
            and not _HAS_PRINTED_COLLECT_MESSAGE
        ):
            print(
                "You acknowledge and agree that the CLI tool may collect usage information, user environment, "
                "and crash reports for the purposes of providing services of functions that are relevant "
                "to use of the CLI tool and product improvements. "
                "To remove this message run 'cdf collect opt-in', "
                "or to stop collecting usage information run 'cdf collect opt-out'."
            )
            _HAS_PRINTED_COLLECT_MESSAGE = True

        try:
            result = execute(*args, **kwargs)
        except Exception as e:
            self._track_command(e)
            raise e
        else:
            self._track_command("success")
        return result

    def warn(self, warning: ToolkitWarning) -> None:
        self.warning_list.append(warning)
        if self.print_warning:
            warning.print_warning()

    def console(self, message: str, prefix: str = "[bold green]INFO:[/] ") -> None:
        if not self.silent:
            print(f"{prefix}{message}")

    def _load_files(
        self,
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        filepaths: list[Path],
        ToolGlobals: CDFToolConfig,
        skip_validation: bool,
    ) -> T_CogniteResourceList:
        loaded_resources = loader.list_write_cls([])
        for filepath in filepaths:
            try:
                resource = loader.load_resource_file(filepath, ToolGlobals)
            except KeyError as e:
                # KeyError means that we are missing a required field in the yaml file.
                raise ToolkitRequiredValueError(
                    f"Failed to load {filepath.name} with {loader.display_name}. Missing required field: {e}."
                    f"\nPlease compare with the API specification at {loader.doc_url()}."
                )
            except TypeError as e:
                raise ToolkitTypeError(
                    f"Failed to load {filepath.name} with {loader.display_name}. Wrong type {e!r}"
                ) from e
            if resource is None:
                # This is intentional. It is, for example, used by the AuthLoader to skip groups with resource scopes.
                continue
            if isinstance(resource, loader.list_write_cls) and not resource:
                self.warn(LowSeverityWarning(f"Skipping {filepath.name}. No data to load."))
                continue

            if isinstance(resource, loader.list_write_cls):
                loaded_resources.extend(resource)
            else:
                loaded_resources.append(resource)
        return loaded_resources

    def _load_files2(
        self,
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        filepaths: list[Path],
        ToolGlobals: CDFToolConfig,
        is_dry_run: bool,
        split_resources: bool = False,
        verbose: bool = False,
    ) -> tuple[T_CogniteResourceList, T_CogniteResourceList, T_CogniteResourceList, list[T_ID]]:
        duplicates: list[T_ID] = []
        resource_by_id: dict[T_ID, dict[str, Any]] = {}
        # Load all resources from files, get ids, and remove duplicates.
        environment_variables = ToolGlobals.environment_variables() if loader.do_environment_variable_injection else {}
        for filepath in filepaths:
            try:
                resource_list = loader.load_resource_file(filepath, environment_variables)
            except YAMLError as e:
                raise ToolkitYAMLFormatError(
                    f"YAML validation error for {filepath.name}: {e}"
                )
            for resource_dict in resource_list:
                identifier = loader.get_id(resource_dict)
                if identifier in resource_by_id:
                    duplicates.append(identifier)
                else:
                    resource_by_id[identifier] = resource_dict

        # Lookup the existing resources in CDF
        try:
            cdf_resources = loader.retrieve(list(resource_by_id.keys()))
        except CogniteAPIError as e:
            self.warn(
                MediumSeverityWarning(
                    f"Failed to retrieve {len(resource_by_id.keys())} of {loader.display_name}. Proceeding assuming not data in CDF. Error {e}."
                )
            )
            cdf_resource_by_id = {}
        else:
            cdf_resource_by_id = {loader.get_id(resource): resource for resource in cdf_resources}
        if not split_resources:
            return loader.list_write_cls([]), duplicates

        to_create, to_update, unchanged = loader.list_write_cls([]), loader.list_write_cls(
            []
        ), loader.list_write_cls([])
        for identifier, local_dict in resource_by_id.items():
            cdf_resource = cdf_resource_by_id.get(identifier)
            local_resource = loader.load_resource(local_dict, is_dry_run)
            if cdf_resource is None:
                to_create.append(local_resource)
                continue
            cdf_dict = loader.dump_resource(cdf_resource, local_dict)
            if cdf_dict == local_dict:
                unchanged.append(local_resource)
                continue
            to_update.append(local_resource)
            if verbose:
                print(
                    Panel(
                        "\n".join(to_diff(cdf_dict, local_dict)),
                        title=f"{loader.display_name}: {identifier}",
                        expand=False,
                    )
                )
        return to_create, to_update, unchanged, duplicates
