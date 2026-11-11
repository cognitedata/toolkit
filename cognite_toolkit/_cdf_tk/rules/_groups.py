from collections import Counter
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Literal

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    Acl,
    AnnotationsAcl,
    AssetsAcl,
    DataSetsAcl,
    EventsAcl,
    GeospatialAcl,
    GeospatialCrsAcl,
    GroupRequest,
    LabelsAcl,
    RelationshipsAcl,
    SecurityCategoriesAcl,
    SequencesAcl,
    TemplateGroupsAcl,
    TemplateInstancesAcl,
    VisionModelAcl,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildWarning,
    Insight,
    InternalValidatorException,
    Recommendation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitWrongResourceError
from cognite_toolkit._cdf_tk.resource_ios import GroupAllScopedIO, GroupIO, GroupResourceScopedIO
from cognite_toolkit._cdf_tk.rules._base import (
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
    quote_identifier,
    with_position,
)

DEPRECATED_FEATURES_URL = "https://docs.cognite.com/cdf/deprecated"

# The deprecated or retired API each ACL gives access to, see DEPRECATED_FEATURES_URL.
DEPRECATED_API_BY_ACL: dict[type[Acl], str] = {
    AnnotationsAcl: "Annotations API",
    AssetsAcl: "Assets API",
    DataSetsAcl: "Data sets API",
    EventsAcl: "Events API",
    GeospatialAcl: "Geospatial API",
    GeospatialCrsAcl: "Geospatial API",
    LabelsAcl: "Labels API",
    RelationshipsAcl: "Relationships API",
    SecurityCategoriesAcl: "Security categories API",
    SequencesAcl: "Sequences API",
    TemplateGroupsAcl: "Templates API",
    TemplateInstancesAcl: "Templates API",
    VisionModelAcl: "Vision API",
}


class _DeprecatedAclUsage(BaseModel):
    source: Path
    group_name: str
    acl_name: str
    api: str
    # The 0-based occurrence of the ACL within the group, used to locate it in the source file.
    occurrence: int


class GroupRuleSet(ToolkitRuleSet):
    """Validates that groups do not grant access to deprecated resources.

    In a DATA_MODELING_ONLY project, such ACLs give a warning. In a HYBRID project, they give a recommendation to
    plan the move away from the deprecated resources.
    """

    RESOURCE_TYPES = frozenset({GroupResourceScopedIO.as_resource_type()})
    CONTEXT = frozenset({"local"})
    DISPLAY_NAME = "Groups"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(
                code="skip",
                message=(
                    "Deprecated ACL validation requires a client. "
                    "Provide client credentials to check groups for ACLs of deprecated resources."
                ),
            )
        return RuleSetStatus(code="ready", message="Will validate that groups do not use ACLs of deprecated resources.")

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        usages: list[_DeprecatedAclUsage] = []
        cruds = (GroupResourceScopedIO(self.client), GroupAllScopedIO(self.client))
        for resource in self.resources:
            if resource.type not in self.RESOURCE_TYPES or not resource.has_valid_syntax:
                continue
            if not isinstance(resource.source, Path):
                continue
            try:
                group = self._load_group(resource, cruds)
            except Exception as error:
                yield InternalValidatorException(
                    message=f"Failed to load group {quote_identifier(resource.identifier)}: {error}",
                    source=str(resource.identifier),
                )
                continue
            occurrences: Counter[str] = Counter()
            for capability in group.capabilities or []:
                acl_name = capability.acl.acl_name
                occurrence = occurrences[acl_name]
                occurrences[acl_name] += 1
                if api := DEPRECATED_API_BY_ACL.get(type(capability.acl)):
                    usages.append(
                        _DeprecatedAclUsage(
                            source=resource.source,
                            group_name=group.name,
                            acl_name=acl_name,
                            api=api,
                            occurrence=occurrence,
                        )
                    )

        if not usages:
            return
        try:
            status = self.client.project.status().this_project.data_modeling_status
        except (ToolkitAPIError, ValueError) as error:
            yield InternalValidatorException(message=f"Failed to retrieve the project status: {error}", source="Groups")
            return
        for usage in usages:
            yield self._create_insight(usage, status)

    @staticmethod
    def _load_group(resource: ResourceRef, cruds: Sequence[GroupIO]) -> GroupRequest:
        """Each group IO only loads groups with its kind of scopes, so the group is loaded by the one accepting it."""
        for crud in cruds:
            try:
                return resource.as_request(crud, is_dry_run=True).request
            except ToolkitWrongResourceError:
                continue
        raise ToolkitWrongResourceError(f"No group IO accepts the group {quote_identifier(resource.identifier)}.")

    @staticmethod
    def _create_insight(usage: _DeprecatedAclUsage, status: Literal["HYBRID", "DATA_MODELING_ONLY"]) -> Insight:
        message = (
            f"Group '{usage.group_name}' has {usage.acl_name!r}, which gives access to the deprecated {usage.api}."
        )
        if status == "DATA_MODELING_ONLY":
            insight: Insight = BuildWarning(
                code="DEPRECATED-CAPABILITY",
                message=f"{message} This project is DATA_MODELING_ONLY, so it should not use deprecated resources.",
                fix=f"Remove {usage.acl_name!r} from the group. See {DEPRECATED_FEATURES_URL} for the replacement.",
                source_file=usage.source,
            )
        else:
            insight = Recommendation(
                code="DEPRECATED-CAPABILITY",
                message=message,
                fix=(
                    f"Plan to move away from the {usage.api} and remove {usage.acl_name!r} from the group. "
                    f"See {DEPRECATED_FEATURES_URL} for the replacement and retirement date."
                ),
                source_file=usage.source,
            )
        return with_position(insight, keys=[usage.acl_name], key_occurrence=usage.occurrence)
