from __future__ import annotations

import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from rich import print

from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError, ToolkitTypeError
from cognite_toolkit._cdf_tk.loaders import (
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import T_ID, T_WritableCogniteResourceList
from cognite_toolkit._cdf_tk.tk_warnings import (
    LowSeverityWarning,
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.tracker import Tracker
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
)


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
        is_collect_command = len(sys.argv) >= 2 and "collect" == sys.argv[1]
        if not self.tracker.opted_in and not self.tracker.opted_out and not is_collect_command:
            print(
                "You acknowledge and agree that the CLI tool may collect usage information, user environment, "
                "and crash reports for the purposes of providing services of functions that are relevant "
                "to use of the CLI tool and product improvements. "
                "To remove this message run 'cdf collect opt-in', "
                "or to stop collecting usage information run 'cdf collect opt-out'."
            )

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
                resource = loader.load_resource(filepath, ToolGlobals, skip_validation)
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
