from __future__ import annotations

from enum import Enum
from functools import lru_cache
from typing import Any, ClassVar

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml


class Flags(Enum):
    MODULES_CMD: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the modules management subapp"}
    INTERNAL: ClassVar[dict[str, Any]] = {"visible": False, "description": "Does nothing"}
    IMPORT_CMD: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the import sub application"}
    ASSETS: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the support for loading assets"}
    TIMESERIES_DUMP: ClassVar[dict[str, Any]] = {
        "visible": True,
        "description": "Enables the support to dump timeseries",
    }
    GRAPHQL: ClassVar[dict[str, Any]] = {
        "visible": True,
        "description": "Enables the support for deploying data models as GraphQL schemas",
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
        return user_settings.get(fflag.name.lower().replace("_", "-"), False)
