from __future__ import annotations

import tempfile
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, ClassVar

import yaml
from rich import print
from rich.table import Table

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError


class Flags(Enum):
    MODULES_CMD: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the modules management subapp"}
    INTERNAL: ClassVar[dict[str, Any]] = {"visible": False, "description": "Does nothing"}
    IMPORT_CMD: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the import sub application"}
    ASSETS: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the support for loading assets"}
    NO_NAMING: ClassVar[dict[str, Any]] = {"visible": True, "description": "Disables the naming convention checks"}
    ROBOTICS: ClassVar[dict[str, Any]] = {"visible": False, "description": "Enables the robotics sub application"}


class FeatureFlag:
    @staticmethod
    def _get_file() -> Path:
        f = Path(tempfile.gettempdir()) / "tk-ff.bin"
        if not f.exists():
            f.write_text("{}")
        return f

    @staticmethod
    def load_user_settings() -> dict[str, bool]:
        return yaml.safe_load(FeatureFlag._get_file().read_text())

    @staticmethod
    def save_user_settings(flag: Flags, enabled: bool) -> None:
        settings = FeatureFlag.load_user_settings()
        settings.update({flag.name: enabled})
        FeatureFlag._get_file().write_text(yaml.dump(settings))
        FeatureFlag.is_enabled.cache_clear()

    @staticmethod
    def reset_user_settings() -> None:
        FeatureFlag._get_file().unlink()
        FeatureFlag.is_enabled.cache_clear()

    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(flag: str | Flags) -> bool:
        if isinstance(flag, str):
            fflag = FeatureFlag.to_flag(flag)
        else:
            fflag = flag

        if not fflag:
            return False

        user_settings = FeatureFlag.load_user_settings()
        return user_settings.get(fflag.name, False)

    @staticmethod
    @lru_cache
    def to_flag(flag: str) -> Flags | None:
        try:
            return Flags[flag.upper().replace("-", "_")]
        except KeyError:
            return None


class FeatureFlagCommand(ToolkitCommand):
    def list(self) -> None:
        user_settings = FeatureFlag.load_user_settings()
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

    def set(self, flag: str, enabled: bool) -> None:
        fflag = FeatureFlag.to_flag(flag)
        if not fflag:
            raise ToolkitRequiredValueError(
                f"Unknown flag: [bold]{flag}[/]. Use the [bold]list[/] command to see available flags"
            )
        FeatureFlag.save_user_settings(fflag, enabled)
        print(f"Feature flag [bold yellow]{flag}[/] has been [bold yellow]{'enabled' if enabled else 'disabled'}[/]")

    def reset(self) -> None:
        FeatureFlag.reset_user_settings()
        print("Feature flags have been reset")
