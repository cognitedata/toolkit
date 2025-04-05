from __future__ import annotations

from enum import Enum
from functools import lru_cache
from typing import Any, ClassVar

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import clean_name


class Flags(Enum):
    # Ruff requires annotations while mypy requires no annotations
    INTERNAL: ClassVar[dict[str, Any]] = {"visible": False, "description": "Does nothing"}  # type: ignore[misc]
    IMPORT_CMD: ClassVar[dict[str, Any]] = {"visible": True, "description": "Enables the import sub application"}  # type: ignore[misc]
    GRAPHQL: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for deploying data models as GraphQL schemas",
    }
    MODULE_REPEAT: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for repeating modules in the config file",
    }
    POPULATE: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables support for the populate command",
    }
    AGENTS: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables support for Atlas Agents and Agent Tools",
    }
    INFIELD: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables support for Infield configs",
    }

    def is_enabled(self) -> bool:
        return FeatureFlag.is_enabled(self)


class FeatureFlag:
    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(flag: Flags) -> bool:
        return CDFToml.load().alpha_flags.get(clean_name(flag.name), False)
