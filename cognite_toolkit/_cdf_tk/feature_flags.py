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
        visible=True,
        description="Enables the support for deploying data models as GraphQL schemas",
    )
    MODULE_REPEAT = FlagMetadata(
        visible=True,
        description="Enables the support for repeating modules in the config file",
    )
    POPULATE = FlagMetadata(
        visible=True,
        description="Enables support for the populate command",
    )
    AGENTS = FlagMetadata(
        visible=True,
        description="Enables support for Atlas Agents and Agent Tools",
    )
    DUMP_EXTENDED = FlagMetadata(
        visible=True,
        description="Enables support for the dumping Location Filters.",
    )
    PROFILE = FlagMetadata(
        visible=True,
        description="Enables support for the profile command",
    )
    INFIELD = FlagMetadata(
        visible=True,
        description="Enables support for Infield configs",
    )
    DUMP_DATA = FlagMetadata(
        visible=True,
        description="Splits the dump command in dump data and dump config",
    )
    EXIT_ON_WARNING = FlagMetadata(
        visible=True,
        description="Enables the exit on warning feature during the build command",
    )
    MIGRATE = FlagMetadata(
        visible=True,
        description="Enables the migrate command",
    )
    EXTERNAL_LIBRARIES = FlagMetadata(
        visible=True,
        description="Enables the support for external libraries in the config file",
    )
    PURGE_INSTANCES = FlagMetadata(
        visible=True,
        description="Enables the cdf purge instances command",
    )
    DOWNLOAD = FlagMetadata(
        visible=True,
        description="Enables the support for the download command",
    )
    UPLOAD = FlagMetadata(
        visible=True,
        description="Enables the cdf upload command",
    )
    SEARCH_CONFIG = FlagMetadata(
        visible=True,
        description="Enables the support for the search config command",
    )

    def is_enabled(self) -> bool:
        return FeatureFlag.is_enabled(self)


class FeatureFlag:
    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(flag: Flags) -> bool:
        return CDFToml.load().alpha_flags.get(clean_name(flag.name), False)
