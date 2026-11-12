from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    Insight,
    InternalValidatorException,
    Recommendation,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    AssetIO,
    DataSetsIO,
    EventIO,
    FileMetadataIO,
    LabelIO,
    RelationshipIO,
    ResourceType,
    SecurityCategoryIO,
    SequenceIO,
    SequenceRowIO,
    TimeSeriesIO,
)
from cognite_toolkit._cdf_tk.rules._base import (
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
    identifier_values,
    quote_identifier,
    with_position,
)

DEPRECATED_FEATURES_URL = "https://docs.cognite.com/cdf/deprecated"
RETIREMENT_DATE = "December 31, 2027"


@dataclass(frozen=True)
class _Deprecation:
    singular: str
    plural: str
    api: str
    # The action required, as described in DEPRECATED_FEATURES_URL.
    mitigation: str


DEPRECATION_BY_RESOURCE_TYPE: dict[ResourceType, _Deprecation] = {
    AssetIO.as_resource_type(): _Deprecation(
        "asset",
        "assets",
        "Assets API",
        "Move asset hierarchies to CogniteAsset in the core data model. "
        "You can upgrade assets with the migration plugin.",
    ),
    EventIO.as_resource_type(): _Deprecation(
        "event",
        "events",
        "Events API",
        "Move high-volume alarms, logs, and notifications to records, and workflow activities to CogniteActivity "
        "or CogniteMaintenanceOrder. You can upgrade events with the migration plugin.",
    ),
    SequenceIO.as_resource_type(): _Deprecation(
        "sequence",
        "sequences",
        "Sequences API",
        "For tables that consumers download as a whole, use files in data modeling. "
        "For multi-column data that needs row-level queries, use records.",
    ),
    SequenceRowIO.as_resource_type(): _Deprecation(
        "sequence row resource",
        "sequence row resources",
        "Sequences API",
        "For tables that consumers download as a whole, use files in data modeling. "
        "For multi-column data that needs row-level queries, use records.",
    ),
    TimeSeriesIO.as_resource_type(): _Deprecation(
        "asset-centric time series",
        "asset-centric time series",
        "asset-centric metadata on the Time series API",
        "Create time series as CogniteTimeSeries instances. You can upgrade time series with the migration plugin.",
    ),
    FileMetadataIO.as_resource_type(): _Deprecation(
        "asset-centric file",
        "asset-centric files",
        "asset-centric metadata on the Files API",
        "Create files as CogniteFile instances. You can upgrade files with the migration plugin.",
    ),
    DataSetsIO.as_resource_type(): _Deprecation(
        "data set",
        "data sets",
        "Data sets API",
        "Scope data with spaces and govern consumption with data products. "
        "You can upgrade data sets with the migration plugin.",
    ),
    LabelIO.as_resource_type(): _Deprecation(
        "label",
        "labels",
        "Labels API",
        "There is no direct replacement. Model labels as a concept in your data model.",
    ),
    RelationshipIO.as_resource_type(): _Deprecation(
        "relationship",
        "relationships",
        "Relationships API",
        "Model connections as edges or direct relations in the data modeling service.",
    ),
    SecurityCategoryIO.as_resource_type(): _Deprecation(
        "security category",
        "security categories",
        "Security categories API",
        "Replace security categories with data products and granular access control.",
    ),
}


class ClassicDeprecationRuleSet(ToolkitRuleSet):
    """Recommends moving away from classic (asset-centric) resources that use deprecated APIs.

    At most one recommendation is given per resource type, pointing to the first resource of that type and
    stating how many resources of that type there are.
    """

    RESOURCE_TYPES = frozenset(DEPRECATION_BY_RESOURCE_TYPE)
    CONTEXT = frozenset({"local"})
    DISPLAY_NAME = "Classic deprecations"

    def get_status(self) -> RuleSetStatus:
        if not Flags.ALPHA_RULES.is_enabled():
            return RuleSetStatus(
                code="skip",
                message=(
                    "Deprecated classic resource validation is an alpha rule. "
                    "Enable the 'alpha-rules' alpha flag to run it."
                ),
            )
        return RuleSetStatus(code="ready", message="Will check for classic resources that use deprecated APIs.")

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if not Flags.ALPHA_RULES.is_enabled():
            return
        first_by_type: dict[ResourceType, tuple[ResourceRef, Path]] = {}
        count_by_type: Counter[ResourceType] = Counter()
        for resource in self.resources:
            if resource.type not in DEPRECATION_BY_RESOURCE_TYPE or not isinstance(resource.source, Path):
                continue
            count_by_type[resource.type] += 1
            first_by_type.setdefault(resource.type, (resource, resource.source))

        for resource_type, deprecation in DEPRECATION_BY_RESOURCE_TYPE.items():
            if resource_type in first_by_type:
                first, source = first_by_type[resource_type]
                yield self._create_recommendation(deprecation, first, source, count_by_type[resource_type])

    @staticmethod
    def _create_recommendation(
        deprecation: _Deprecation, first: ResourceRef, source: Path, count: int
    ) -> Recommendation:
        if count == 1:
            found = f"Found 1 {deprecation.singular}, {quote_identifier(first.identifier)}."
        else:
            found = f"Found {count} {deprecation.plural}, the first is {quote_identifier(first.identifier)}."
        recommendation = Recommendation(
            code="DEPRECATED-RESOURCE",
            message=f"{found} The {deprecation.api} is deprecated and will be retired on {RETIREMENT_DATE}.",
            fix=(
                f"{deprecation.mitigation} Complete the move before {RETIREMENT_DATE}. "
                f"See {DEPRECATED_FEATURES_URL} for details."
            ),
            source_file=source,
            alpha=True,
        )
        return with_position(recommendation, values=identifier_values(first.identifier), variables=first.variables)
