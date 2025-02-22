from __future__ import annotations

import os
import platform
import sys
import tempfile
import threading
import uuid
from collections import Counter
from contextlib import suppress
from functools import cached_property
from pathlib import Path
from typing import Any

from mixpanel import Consumer, Mixpanel, MixpanelException

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import IN_BROWSER
from cognite_toolkit._cdf_tk.data_classes._built_modules import BuiltModule
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList
from cognite_toolkit._cdf_tk.utils import get_cicd_environment
from cognite_toolkit._version import __version__

_COGNITE_TOOLKIT_MIXPANEL_TOKEN: str = "9afc120ac61d408c81009ea7dd280a38"


class Tracker:
    def __init__(self, skip_tracking: bool = False) -> None:
        self.user_command = "".join(sys.argv[1:])
        self.mp = Mixpanel(_COGNITE_TOOLKIT_MIXPANEL_TOKEN, consumer=Consumer(api_host="api-eu.mixpanel.com"))
        self._opt_status_file = Path(tempfile.gettempdir()) / "tk-opt-status.bin"
        self.skip_tracking = self.opted_out or skip_tracking
        self._cdf_toml = CDFToml.load()

    @cached_property
    def _opt_status(self) -> str:
        if self._opt_status_file.exists():
            return self._opt_status_file.read_text()
        return ""

    @property
    def opted_out(self) -> bool:
        return self._opt_status == "opted-out"

    @property
    def opted_in(self) -> bool:
        return self._opt_status == "opted-in"

    def track_cli_command(self, warning_list: WarningList[ToolkitWarning], result: str | Exception, cmd: str) -> bool:
        warning_count = Counter([type(w).__name__ for w in warning_list])

        warning_details: dict[str, str | int] = {}
        for no, (warning, count) in enumerate(warning_count.most_common(3), 1):
            warning_details[f"warningMostCommon{no}Count"] = count
            warning_details[f"warningMostCommon{no}Name"] = warning

        positional_args, optional_args = self._parse_sys_args()
        event_information = {
            "userInput": self.user_command,
            "toolkitVersion": __version__,
            "$os": platform.system(),
            "pythonVersion": platform.python_version(),
            "CICD": self._cicd,
            "warningTotalCount": len(warning_list),
            **warning_details,
            "result": type(result).__name__ if isinstance(result, Exception) else result,
            "error": str(result) if isinstance(result, Exception) else "",
            **positional_args,
            **optional_args,
            **{f"alphaFlag-{name}": value for name, value in self._cdf_toml.alpha_flags.items()},
            **{f"plugin-{name}": value for name, value in self._cdf_toml.plugins.items()},
        }

        return self._track(f"command{cmd.capitalize()}", event_information)

    def track_module_build(self, module: BuiltModule) -> bool:
        event_information = {
            "module": module.name,
            "location_path": module.location.path.as_posix(),
            "warning_count": module.warning_count,
            "status": module.status,
            **{resource_type: len(resource_build) for resource_type, resource_build in module.resources.items()},
        }
        return self._track("moduleBuild", event_information)

    def _track(self, event_name: str, event_information: dict[str, Any]) -> bool:
        if self.skip_tracking or not self.opted_in or "PYTEST_CURRENT_TEST" in os.environ:
            return False

        distinct_id = self.get_distinct_id()

        def track() -> None:
            # If we are unable to connect to Mixpanel, we don't want to crash the program
            with suppress(ConnectionError, MixpanelException):
                self.mp.track(
                    distinct_id,
                    event_name,
                    event_information,
                )

        if IN_BROWSER:
            # Pyodide does not support threading
            track()
        else:
            thread = threading.Thread(
                target=track,
                daemon=False,
            )
            thread.start()

        return True

    def get_distinct_id(self) -> str:
        cache = Path(tempfile.gettempdir()) / "tk-distinct-id.bin"
        cicd = self._cicd
        if cache.exists():
            return cache.read_text()

        distinct_id = f"{cicd}-{platform.system()}-{platform.python_version()}-{uuid.uuid4()!s}"
        cache.write_text(distinct_id)
        with suppress(ConnectionError, MixpanelException):
            self.mp.people_set(
                distinct_id,
                {
                    "$os": platform.system(),
                    "$python_version": platform.python_version(),
                    "$distinct_id": distinct_id,
                    "CICD": self._cicd,
                },
            )
        return distinct_id

    @staticmethod
    def _parse_sys_args() -> tuple[dict[str, str], dict[str, str | bool]]:
        optional_args: dict[str, str | bool] = {}
        positional_args: dict[str, str] = {}
        last_key: str | None = None
        if sys.argv and len(sys.argv) > 1:
            for arg in sys.argv[1:]:
                if arg.startswith("--") and "=" in arg:
                    if last_key:
                        optional_args[last_key] = True
                    key, value = arg.split("=", maxsplit=1)
                    optional_args[key.removeprefix("--")] = value
                elif arg.startswith("--"):
                    if last_key:
                        optional_args[last_key] = True
                    last_key = arg.removeprefix("--")
                elif last_key:
                    optional_args[last_key] = arg
                    last_key = None
                else:
                    positional_args[f"positionalArg{len(positional_args)}"] = arg

            if last_key:
                optional_args[last_key] = True
        return positional_args, optional_args

    @property
    def _cicd(self) -> str:
        return get_cicd_environment()

    def enable(self) -> None:
        self._opt_status_file.write_text("opted-in")

    def disable(self) -> None:
        self._opt_status_file.write_text("opted-out")
