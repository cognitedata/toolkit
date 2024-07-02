from __future__ import annotations

import os
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from rich import print

from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError, ToolkitYAMLFormatError
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
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False):
        self.print_warning = print_warning
        if len(sys.argv) > 1:
            self.user_command = f"cdf-tk {' '.join(sys.argv[1:])}"
        else:
            self.user_command = "cdf-tk"
        self.warning_list = WarningList[ToolkitWarning]()
        self.tracker = Tracker(self.user_command)
        self.skip_tracking = self.tracker.opted_out or skip_tracking

    def _track_command(self, result: str | Exception) -> None:
        if self.skip_tracking or "PYTEST_CURRENT_TEST" in os.environ:
            return
        self.tracker.track_command(self.warning_list, result, type(self).__name__.removesuffix("Command"))

    def run(self, execute: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if (
            not self.tracker.opted_in
            and not self.tracker.opted_out
            and not self.user_command.startswith("cdf-tk collect")
        ):
            print(
                "You acknowledge and agree that the CLI tool may collect usage information, user environment, "
                "and crash reports for the purposes of providing services of functions that are relevant "
                "to use of the CLI tool and product improvements. "
                "To remove this message run 'cdf-tk collect opt-in', "
                "or to stop collecting usage information run 'cdf-tk collect opt-out'."
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
            prefix = warning.severity.prefix
            end = "\n" + " " * ((warning.severity.prefix_length + 1) // 2)
            message = warning.get_message().replace("\n", end)
            print(prefix, message)

    def _load_files(
        self,
        loader: ResourceLoader[
            T_ID, T_WriteClass, T_WritableCogniteResource, T_CogniteResourceList, T_WritableCogniteResourceList
        ],
        filepaths: list[Path],
        ToolGlobals: CDFToolConfig,
        skip_validation: bool,
    ) -> T_CogniteResourceList:
        loaded_resources = loader.create_empty_of(loader.list_write_cls([]))
        for filepath in filepaths:
            try:
                resource = loader.load_resource(filepath, ToolGlobals, skip_validation)
            except KeyError as e:
                # KeyError means that we are missing a required field in the yaml file.
                raise ToolkitRequiredValueError(
                    f"Failed to load {filepath.name} with {loader.display_name}. Missing required field: {e}."
                    f"\nPlease compare with the API specification at {loader.doc_url()}."
                )
            except Exception as e:
                raise ToolkitYAMLFormatError(
                    f"Failed to load {filepath.name} with {loader.display_name}. Error: {e!r}."
                )
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
