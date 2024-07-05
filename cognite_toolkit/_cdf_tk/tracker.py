from __future__ import annotations

import os
import platform
import sys
import tempfile
import threading
import uuid
from collections import Counter
from functools import cached_property
from pathlib import Path

from mixpanel import Consumer, Mixpanel

from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList
from cognite_toolkit._version import __version__

_COGNITE_TOOLKIT_MIXPANEL_TOKEN: str = "9afc120ac61d408c81009ea7dd280a38"


class Tracker:
    def __init__(self, user_command: str) -> None:
        self.user_command = user_command
        self.mp = Mixpanel(_COGNITE_TOOLKIT_MIXPANEL_TOKEN, consumer=Consumer(api_host="api-eu.mixpanel.com"))
        self._opt_status_file = Path(tempfile.gettempdir()) / "tk-opt-status.bin"

    def track_command(self, warning_list: WarningList[ToolkitWarning], result: str | Exception, cmd: str) -> None:
        distinct_id = self.get_distinct_id()
        positional_args, optional_args = self._parse_sys_args()
        warning_count = Counter([type(w).__name__ for w in warning_list])

        warning_details: dict[str, str | int] = {}
        for no, (warning, count) in enumerate(warning_count.most_common(3), 1):
            warning_details[f"warningMostCommon{no}Count"] = count
            warning_details[f"warningMostCommon{no}Name"] = warning

        thread = threading.Thread(
            target=lambda: self.mp.track(
                distinct_id,
                f"command{cmd.capitalize()}",
                {
                    "userInput": self.user_command,
                    "toolkitVersion": __version__,
                    "warningTotalCount": len(warning_list),
                    **warning_details,
                    "result": type(result).__name__ if isinstance(result, Exception) else result,
                    "error": str(result) if isinstance(result, Exception) else "",
                    "$os": platform.system(),
                    "pythonVersion": platform.python_version(),
                    "CICD": self._cicd,
                    **positional_args,
                    **optional_args,
                },
            ),
            daemon=False,
        )
        thread.start()

    def get_distinct_id(self) -> str:
        cache = Path(tempfile.gettempdir()) / "tk-distinct-id.bin"
        cicd = self._cicd
        if cache.exists():
            return cache.read_text()

        distinct_id = f"{cicd}-{platform.system()}-{platform.python_version()}-{uuid.uuid4()!s}"
        cache.write_text(distinct_id)
        self.mp.people_set(
            distinct_id,
            {
                "$os": platform.system(),
                "$python_version": platform.python_version(),
                "$distinct_id": distinct_id,
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
        if "CI" in os.environ and os.getenv("GITHUB_ACTIONS"):
            return "github"
        if os.getenv("GITLAB_CI"):
            return "gitlab"
        if "CI" in os.environ and "BITBUCKET_BUILD_NUMBER" in os.environ:
            return "bitbucket"
        if os.getenv("CIRCLECI"):
            return "circleci"
        if os.getenv("TRAVIS"):
            return "travis"
        if "TF_BUILD" in os.environ:
            return "azure"

        return "local"

    def enable(self) -> None:
        self._opt_status_file.write_text("opted-in")

    def disable(self) -> None:
        self._opt_status_file.write_text("opted-out")

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
