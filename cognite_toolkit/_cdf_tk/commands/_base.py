from __future__ import annotations

import traceback
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResourceList
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.load import (
    ResourceLoader,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    LowSeverityWarning,
    ToolkitWarning,
    WarningList,
)
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
)


class ToolkitCommand:
    def __init__(self, print_warning: bool = True):
        self.print_warning = print_warning
        self.warning_list = WarningList[ToolkitWarning]()

    def warn(self, warning: ToolkitWarning) -> None:
        self.warning_list.append(warning)
        if self.print_warning:
            print(warning.get_message())

    def _load_files(
        self,
        loader: ResourceLoader,
        filepaths: list[Path],
        ToolGlobals: CDFToolConfig,
        skip_validation: bool,
        verbose: bool = False,
    ) -> T_CogniteResourceList | None:
        loaded_resources = loader.create_empty_of(loader.list_write_cls([]))
        for filepath in filepaths:
            try:
                resource = loader.load_resource(filepath, ToolGlobals, skip_validation)
            except KeyError as e:
                # KeyError means that we are missing a required field in the yaml file.
                print(
                    f"[bold red]ERROR:[/] Failed to load {filepath.name} with {loader.display_name}. Missing required field: {e}."
                    f"[bold red]ERROR:[/] Please compare with the API specification at {loader.doc_url()}."
                )
                return None
            except Exception as e:
                print(f"[bold red]ERROR:[/] Failed to load {filepath.name} with {loader.display_name}. Error: {e!r}.")
                if verbose:
                    print(Panel(traceback.format_exc()))
                return None
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
