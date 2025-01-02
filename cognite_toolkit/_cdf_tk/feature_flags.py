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
    ADO_PIPELINES: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables creation of Azure DevOps pipelines in repo init command",
    }
    REQUIRE_KIND: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Require kind in all config file names. For example, `my.FileMetadata.yaml`",
    }
    RUN_WORKFLOW: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for running workflows",
    }
    STREAMLIT: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for deploying Streamlit apps",
    }
    FORCE_UPDATE: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": False,
        "description": "Adding the --force-update flag to the deploy command",
    }
    MODULE_REPEAT: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for repeating modules in the config file",
    }
    EDGES: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for creating edges between nodes",
    }
    CLASSIC: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for events and relationships",
    }
    MODULE_PULL: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for pulling modules from the CDF",
    }
    PURGE: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables the support for purging datasets and spaces",
    }
    BUILD_OFFLINE: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Enables builds without comparing against CDF project for missing dependencies.",
    }
    FUNCTION_MULTI_FILE_HASH: ClassVar[dict[str, Any]] = {  # type: ignore[misc]
        "visible": True,
        "description": "Toolkit will has each individual file in a multi-file function. This is useful when"
        "you want to inspect why a function needs to redeploy.",
    }

    def is_enabled(self) -> bool:
        return FeatureFlag.is_enabled(self)


class FeatureFlag:
    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(flag: Flags) -> bool:
        return CDFToml.load().alpha_flags.get(clean_name(flag.name), False)
