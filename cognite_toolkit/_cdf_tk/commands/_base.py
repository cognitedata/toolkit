from __future__ import annotations

from pathlib import Path

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
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
)


class ToolkitCommand:
    def __init__(self, print_warning: bool = True, user_command: str | None = None):
        self.print_warning = print_warning
        self.user_command = user_command
        self.warning_list = WarningList[ToolkitWarning]()

    def warn(self, warning: ToolkitWarning) -> None:
        self.warning_list.append(warning)
        if self.print_warning:
            print(warning.get_message())

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
