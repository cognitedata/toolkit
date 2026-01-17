from dataclasses import dataclass
from enum import Enum
from functools import lru_cache

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import clean_name


@dataclass(frozen=True)
class FlagMetadata:
    visible: bool
    description: str


class Flags(Enum):
    INTERNAL = FlagMetadata(
        visible=False,
        description="Does nothing",
    )
    IMPORT_CMD = FlagMetadata(
        visible=True,
        description="Enables the import sub application",
    )
    GRAPHQL = FlagMetadata(
        visible=False,
        description="Enables the support for deploying data models as GraphQL schemas",
    )
    MODULE_REPEAT = FlagMetadata(
        visible=True,
        description="Enables the support for repeating modules in the config file",
    )
    PROFILE = FlagMetadata(
        visible=True,
        description="Enables support for the profile command",
    )
    INFIELD = FlagMetadata(
        visible=True,
        description="Enables support for Infield configs",
    )
    MIGRATE = FlagMetadata(
        visible=True,
        description="Enables the migrate command",
    )
    STREAMS = FlagMetadata(
        visible=True,
        description="Enables the support for the streams resources",
    )
    v08 = FlagMetadata(
        visible=False,
        description="Enables features planned for Cognite Toolkit version 0.8.0",
    )
    CREATE = FlagMetadata(
        visible=True,
        description="Enables the support for the resources create command under dev plugin",
    )
    EXTEND_DOWNLOAD = FlagMetadata(
        visible=True,
        description="Enables extended download to support downloading file content and datapoints",
    )
    EXTEND_UPLOAD = FlagMetadata(
        visible=True,
        description="Enables extended upload to support uploading individual files",
    )
    SIMULATORS = FlagMetadata(
        visible=True,
        description="Enables the support for simulator model resources",
    )
    FUNCTION_REQUIREMENTS_VALIDATION = FlagMetadata(
        visible=True,
        description="Enables validation of function requirements.txt during build using pip dry-run",
    )

    def is_enabled(self) -> bool:
        return FeatureFlag.is_enabled(self)


class FeatureFlag:
    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(flag: Flags) -> bool:
        return CDFToml.load().alpha_flags.get(clean_name(flag.name), False)

    @staticmethod
    def flush() -> None:
        FeatureFlag.is_enabled.cache_clear()
