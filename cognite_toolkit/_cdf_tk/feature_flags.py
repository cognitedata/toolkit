from dataclasses import dataclass
from enum import Enum
from functools import lru_cache

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import clean_name


@dataclass(frozen=True)
class FlagMetadata:
    """Metadata for a feature flag.

    Attributes:
        visible: If True, the flag is shown in 'cdf about' even when disabled,
            allowing users to discover it. If False, the flag is hidden unless
            explicitly enabled in cdf.toml.
        description: Human-readable description of what the flag enables.
    """

    visible: bool
    description: str


class Flags(Enum):
    INTERNAL = FlagMetadata(
        visible=False,
        description="Does nothing",
    )
    ALPHA_RULES = FlagMetadata(
        visible=False,
        description="Enables alpha rules",
    )
    IMPORT_CMD = FlagMetadata(
        visible=True,
        description="Enables the import sub application",
    )
    GRAPHQL = FlagMetadata(
        visible=False,
        description="Enables the support for deploying data models as GraphQL schemas",
    )
    INFIELD = FlagMetadata(
        visible=True,
        description="Enables support for Infield configs",
    )
    MIGRATE = FlagMetadata(
        visible=True,
        description="Enables the migrate command",
    )
    CREATE = FlagMetadata(
        visible=True,
        description="Enables support for the resources create command under dev plugin",
    )
    FUNCTION_REQUIREMENTS_VALIDATION = FlagMetadata(
        visible=True,
        description="Enables validation of function requirements.txt during build using pip dry-run",
    )
    DATA_PRODUCTS = FlagMetadata(
        visible=True,
        description="Enables support for data product and rule set resources and workflow task lineage annotations",
    )
    MODULES_LIST_JSON = FlagMetadata(
        visible=True,
        description="Enables JSON output format for the modules list command",
    )
    ENTITY_MATCHING = FlagMetadata(
        visible=True,
        description="Enables the entity-matching command family under the dev plugin",
    )
    CUSTOM_APPS = FlagMetadata(
        visible=True,
        description="Enables support for custom app resources (App Hosting API deployment)",
    )
    INFIELD_LOCATION_SPLIT = FlagMetadata(
        visible=True,
        description="Enables location-split InField data migration in order to untangle shared legacy instance spaces",
    )
    CATALOG_DATASET = FlagMetadata(
        visible=True,
        description="Enables support for catalog dataset resources",
    )
    API = FlagMetadata(
        visible=True,
        description="Enables the api command family for calling CDF APIs",
    )
    FUNCTION_APPS = FlagMetadata(
        visible=False,
        description="Enables local Function App development commands",
    )
    MULTI_CONFIGS = FlagMetadata(
        visible=True,
        description="Enables support for passing multiple config.<>.yaml files into the build command",
    )
    GIT_DIFF_SELECT = FlagMetadata(
        visible=True,
        description="Enables support for selecting modules in a config.<>.yaml based on git diff.",
    )

    def is_enabled(self) -> bool:
        return FeatureFlag.is_enabled(self)


class FeatureFlag:
    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(flag: Flags) -> bool:
        return CDFToml.load_module_context().alpha_flags.get(clean_name(flag.name), False)

    @staticmethod
    def flush() -> None:
        FeatureFlag.is_enabled.cache_clear()
