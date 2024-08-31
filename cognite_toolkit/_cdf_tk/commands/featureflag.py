from __future__ import annotations

from enum import Enum
from functools import lru_cache
from typing import Any, ClassVar

from rich import print
from rich.table import Table

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand


class Flags(Enum):
    MODULES_CMD: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the modules management subapp"}
    INTERNAL: ClassVar[dict[str, Any]] = {"visible": False, "description": "Does nothing"}
    IMPORT_CMD: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the import sub application"}
    ASSETS: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the support for loading assets"}
    TIMESERIES_DUMP: ClassVar[dict[str, Any]] = {
        "visible": True,
        "description": "Enables the support to dump timeseries",
    }


class FeatureFlag:
    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(flag: str | Flags) -> bool:
        if isinstance(flag, str):
            try:
                fflag = Flags[flag.upper().replace("-", "_")]
            except KeyError:
                return False
        else:
            fflag = flag

        if not fflag:
            return False

        user_settings = CDFToml.load().cdf.feature_flags
        return user_settings.get(fflag.name, False)


class FeatureFlagCommand(ToolkitCommand):
    @staticmethod
    def list() -> None:
        user_settings = CDFToml.load().cdf.feature_flags
        table = Table(title="feature flags")
        table.add_column("Name", justify="left")
        table.add_column("Description", justify="left")
        table.add_column("Status", justify="left")

        for flag in Flags:
            is_enabled = user_settings.get(flag.name, False)
            if is_enabled or flag.value.get("visible", False):
                table.add_row(
                    flag.name,
                    str(flag.value.get("description", "")),
                    "enabled" if is_enabled else "disabled",
                    style="yellow" if is_enabled else "",
                )
        print(table)
