import traceback
from pathlib import Path

from cognite.client.data_classes._base import T_CogniteResourceList
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.load import ResourceLoader
from cognite_toolkit._cdf_tk.load._base_loaders import T_ID
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class ToolkitCommand:
    def __init__(self, print_warning: bool = True):
        self.print_warning = print_warning
        self.warning_list = WarningList[ToolkitWarning]()

    def warn(self, warning: ToolkitWarning) -> None:
        self.warning_list.append(warning)
        if self.print_warning:
            print(warning.get_message())


class LoaderCommand(ToolkitCommand):
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
                print(f"[bold yellow]WARNING:[/] Skipping {filepath.name}. No data to load.")
                continue

            if isinstance(resource, loader.list_write_cls):
                loaded_resources.extend(resource)
            else:
                loaded_resources.append(resource)
        return loaded_resources

    @staticmethod
    def _print_ids_or_length(resource_ids: SequenceNotStr[T_ID], limit: int = 10) -> str:
        if len(resource_ids) == 1:
            return f"{resource_ids[0]!r}"
        elif len(resource_ids) <= limit:
            return f"{resource_ids}"
        else:
            return f"{len(resource_ids)} items"

    def _remove_duplicates(
        self, loaded_resources: T_CogniteResourceList, loader: ResourceLoader
    ) -> tuple[T_CogniteResourceList, list[T_ID]]:
        seen: set[T_ID] = set()
        output = loader.create_empty_of(loaded_resources)
        duplicates: list[T_ID] = []
        for item in loaded_resources:
            identifier = loader.get_id(item)
            if identifier not in seen:
                output.append(item)
                seen.add(identifier)
            else:
                duplicates.append(identifier)
        return output, duplicates
