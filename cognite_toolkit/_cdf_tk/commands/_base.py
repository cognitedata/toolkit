from __future__ import annotations

import platform
import sys
import tempfile
import uuid
from collections.abc import Callable
from pathlib import Path
from typing import Any

from cognite.client.data_classes._base import T_CogniteResourceList, T_WritableCogniteResource, T_WriteClass
from mixpanel import Consumer, Mixpanel
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
from cognite_toolkit._version import __version__

_COGNITE_TOOLKIT_MIXPANEL_TOKEN: str | None = "9afc120ac61d408c81009ea7dd280a38"


class ToolkitCommand:
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False):
        self.print_warning = print_warning
        if len(sys.argv) > 1:
            self.user_command = f"cdf-tk {' '.join(sys.argv[1:])}"
        else:
            self.user_command = "cdf-tk"
        self.warning_list = WarningList[ToolkitWarning]()
        self.skip_tracking = skip_tracking

    def _track_command(self, result: str | Exception) -> None:
        if self.skip_tracking or _COGNITE_TOOLKIT_MIXPANEL_TOKEN is None:
            return
        mp = Mixpanel(_COGNITE_TOOLKIT_MIXPANEL_TOKEN, consumer=Consumer(api_host="api-eu.mixpanel.com"))
        cache = Path(tempfile.gettempdir()) / "tk-distinct-id.bin"
        if cache.exists():
            distinct_id = cache.read_text()
        else:
            distinct_id = f"{platform.system()}-{platform.python_version()}-{uuid.uuid4()!s}"
            cache.write_text(distinct_id)
            mp.people_set(
                distinct_id,
                {
                    "$os": platform.system(),
                    "$os_version": platform.version(),
                    "$os_release": platform.release(),
                    "$python_version": platform.python_version(),
                    "$distinct_id": distinct_id,
                },
            )

        optional_args: dict[str, str] = {}
        positional_args: list[str] = []
        last_key: str | None = None
        if sys.argv and len(sys.argv) > 1:
            for arg in sys.argv[1:]:
                if arg.startswith("--") and "=" in arg:
                    if last_key:
                        optional_args[last_key] = ""
                    key, value = arg.split("=", maxsplit=1)
                    optional_args[key.removeprefix("--")] = value
                elif arg.startswith("--"):
                    if last_key:
                        optional_args[last_key] = ""
                    last_key = arg.removeprefix("--")
                elif last_key:
                    optional_args[last_key] = arg
                    last_key = None
                else:
                    positional_args.append(arg)

        cmd = type(self).__name__.removesuffix("Command")
        mp.track(
            distinct_id,
            f"command{cmd.capitalize()}",
            {
                "userInput": self.user_command,
                "toolkitVersion": __version__,
                "warningCount": len(self.warning_list),
                "result": type(result).__name__ if isinstance(result, Exception) else result,
                "error": str(result) if isinstance(result, Exception) else "",
                "os": platform.system(),
                "osVersion": platform.version(),
                "osRelease": platform.release(),
                "pythonVersion": platform.python_version(),
                "positionalArgs": positional_args,
                **optional_args,
            },
        )

    def run(self, execute: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
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
