from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self

from pydantic import Discriminator, Field, Tag, field_validator, model_validator
from pydantic.functional_validators import BeforeValidator

from cognite_toolkit._cdf_tk.client.identifiers import NameId
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupAttributes
from cognite_toolkit._cdf_tk.feature_flags import Flags

from .base import ToolkitResource
from .capabilities import AllScope, CapabilityType, Scope, TableScope, UnknownCapability

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.data_classes._insights import BuildWarning, Recommendation
    from cognite_toolkit._cdf_tk.resource_ios import ResourceType


class BaseGroupYAML(ToolkitResource):
    group_type: str = Field(
        exclude=True
    )  # Not part of the YAML, but used to determine which subclass to use for validation
    name: str
    capabilities: list[CapabilityType] | None = None
    metadata: dict[str, str] | None = None
    attributes: GroupAttributes | None = None

    def as_id(self) -> NameId:
        return NameId(name=self.name)

    def extra_insights(self, source_file: Path, resource_type: "ResourceType") -> "list[BuildWarning | Recommendation]":
        return [
            *self._unrecognized_capability_warnings(source_file),
            *self._redundant_capability_recommendations(source_file, resource_type),
        ]

    def _unrecognized_capability_warnings(self, source_file: Path) -> "list[BuildWarning]":
        """Return warnings for capabilities that are not recognized by the toolkit."""
        # Lazy import to avoid circular dependency (yaml_classes → commands.build_v2 → resource_ios → yaml_classes).
        from cognite_toolkit._cdf_tk.data_classes._insights import BuildWarning
        from cognite_toolkit._cdf_tk.rules._base import with_position

        capabilities = self.capabilities or []
        return [
            with_position(
                BuildWarning(
                    code="UNRECOGNIZED-VALUE",
                    message=(
                        f"'{cap.original_name}' is not a known capability name. "
                        "It will be deployed as-is, but may be rejected by CDF."
                    ),
                    source_file=source_file,
                    fix="Compare the YAML with reference documentation. The resource will still be deployed.",
                ),
                keys=[cap.original_name],
                key_occurrence=_acl_occurrence(capabilities, position),
            )
            for position, cap in enumerate(capabilities)
            if isinstance(cap, UnknownCapability)
        ]

    def _redundant_capability_recommendations(
        self, source_file: Path, resource_type: "ResourceType"
    ) -> "list[Recommendation]":
        """Recommend dropping ACLs whose actions and scope are already granted by other ACLs.

        An ACL is redundant when every action it grants is contained in the scopes of the group's
        other ACLs of the same type. A space-scoped ``dataModelInstancesAcl`` is redundant when
        another ``dataModelInstancesAcl`` grants the same actions on ``all``.
        """
        from cognite_toolkit._cdf_tk.data_classes._insights import Recommendation
        from cognite_toolkit._cdf_tk.rules._base import with_position

        entries_by_acl: dict[str, list[_AclEntry]] = defaultdict(list)
        for position, capability in enumerate(self.capabilities or []):
            # Unknown ACLs are preserved as-is. Their actions are not modeled, so they are not
            # compared and they do not count as covering another ACL.
            if isinstance(capability, UnknownCapability):
                continue
            actions: list[str] = list(dict.fromkeys(capability.actions))
            grants = _scope_grants(capability.scope)
            if not actions or not grants:
                continue
            entries_by_acl[capability.capability_name].append(
                _AclEntry(position, capability.capability_name, actions, capability.scope, grants)
            )

        redundant = sorted(
            (finding for entries in entries_by_acl.values() for finding in _select_redundant(entries)),
            key=lambda finding: finding[0].position,
        )
        return [
            with_position(
                Recommendation(
                    code="REDUNDANT-PROPERTY",
                    message=_redundant_message(resource_type.kind, self.name, entry, covered_actions),
                    fix=_redundant_fix(entry, covered_actions),
                    source_file=source_file,
                    alpha=True,
                ),
                keys=[entry.label],
                key_occurrence=_acl_occurrence(self.capabilities or [], entry.position),
            )
            for entry, covered_actions in redundant
        ]


class ExternalGroupYAML(BaseGroupYAML):
    group_type: Literal["external"] = Field("external", exclude=True)
    source_id: str


class CDFGroupYAML(BaseGroupYAML):
    group_type: Literal["cdf"] = Field("cdf", exclude=True)
    members: list[str] | Literal["allUserAccounts", "emails"]
    member_emails: list[str] | None = None

    @model_validator(mode="after")
    def _validate_members_and_member_emails(self) -> Self:
        if self.members == "emails" and not self.member_emails:
            raise ValueError(
                "Invalid group definition: 'members' is set to 'emails', but no 'memberEmails' are provided."
            )
        if self.member_emails is not None and self.members != "emails":
            raise ValueError(
                "Invalid group definition: 'memberEmails' can only be specified when 'members' is set to 'emails'."
            )
        return self

    @field_validator("member_emails")
    @classmethod
    def _alpha_flag_enables(cls, v: list[str] | None) -> list[str] | None:
        if v is not None and not Flags.CDF_GROUP_EMAILS.is_enabled():
            raise ValueError(
                "The 'memberEmails' field is currently behind an alpha feature flag and is not enabled. "
                "Please enable the flag `cdf-group-emails` to use this feature, or remove the 'memberEmails' "
                "field from the group definition."
            )
        return v


def _check_group_definition(data: Any) -> Any:
    if isinstance(data, dict) and "sourceId" in data and "members" in data:
        raise ValueError(
            "Invalid group definition: Cannot have both 'sourceId' and 'members'. Please specify only one."
        )
    return data


def _discriminate_group_type(data: Any) -> str | None:
    """The group type is determined by the presence of 'sourceId' (external) or 'members' (CDF)."""
    if isinstance(data, BaseGroupYAML):
        return data.group_type
    if isinstance(data, dict):
        if "sourceId" in data:
            return "external"
        elif "members" in data:
            return "cdf"
    return None


GroupYAML = Annotated[
    Annotated[CDFGroupYAML, Tag("cdf")] | Annotated[ExternalGroupYAML, Tag("external")],
    Discriminator(
        _discriminate_group_type,
        custom_error_type="missing_group_type",
        custom_error_message="Missing required field: Either 'sourceId' or 'members'",
    ),
    BeforeValidator(_check_group_definition),
]


_AccessPath = tuple[Any, ...]


@dataclass(frozen=True)
class _AclEntry:
    position: int
    label: str
    actions: list[str]
    scope: Scope
    grants: frozenset[_AccessPath]


def _yaml_key(capability: CapabilityType) -> str:
    """The key of the ACL in the YAML file."""
    return capability.original_name if isinstance(capability, UnknownCapability) else capability.capability_name


def _acl_occurrence(capabilities: list[CapabilityType], position: int) -> int:
    """How many ACLs with the same key come before the ACL at the position."""
    yaml_key = _yaml_key(capabilities[position])
    return sum(1 for earlier in capabilities[:position] if _yaml_key(earlier) == yaml_key)


def _scope_grants(scope: Scope) -> frozenset[_AccessPath]:
    """Express a scope as the access paths it grants.

    A path grants everything below it: ``all`` is the empty path, and a ``tableScope`` database
    without listed tables grants every table in that database.
    """
    if isinstance(scope, AllScope):
        return frozenset({()})
    if isinstance(scope, TableScope):
        return frozenset(
            path
            for db, tables in scope.dbs_to_tables.items()
            for path in ([(scope.scope_name, db, table) for table in tables] or [(scope.scope_name, db)])
        )
    fields = [name for name in type(scope).model_fields if name != "scope_name"]
    if not fields:
        return frozenset({(scope.scope_name,)})
    return frozenset((scope.scope_name, name, value) for name in fields for value in getattr(scope, name))


def _is_granted(path: _AccessPath, granted: set[_AccessPath]) -> bool:
    return any(path[:end] in granted for end in range(len(path) + 1))


def _covered_actions(entry: _AclEntry, others: list[_AclEntry]) -> list[str]:
    covered: list[str] = []
    for action in entry.actions:
        granted = {path for other in others if action in other.actions for path in other.grants}
        if all(_is_granted(path, granted) for path in entry.grants):
            covered.append(action)
    return covered


def _select_redundant(entries: list[_AclEntry]) -> list[tuple[_AclEntry, list[str]]]:
    """Pick ACLs that can be removed without changing the access the group grants.

    Whole ACLs are removed first, latest first, so an exact duplicate keeps the earliest entry.
    Actions covered only in part are reported on the ACLs that remain.
    """
    kept = list(entries)
    redundant: list[tuple[_AclEntry, list[str]]] = []
    for entry in reversed(entries):
        covered = _covered_actions(entry, [other for other in kept if other is not entry])
        if covered == entry.actions:
            kept.remove(entry)
            redundant.append((entry, covered))

    for entry in kept:
        if covered := _covered_actions(entry, [other for other in kept if other is not entry]):
            redundant.append((entry, covered))
    return redundant


def _join_words(items: list[str]) -> str:
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])}, and {items[-1]}"


def _article(word: str) -> str:
    return "an" if word[:1].lower() in {"a", "e", "i", "o", "u"} else "a"


def _actions_phrase(actions: list[str]) -> str:
    return f"{_join_words(actions)} {'action' if len(actions) == 1 else 'actions'}"


def _format_scope(scope: Scope) -> str:
    body = scope.model_dump(by_alias=True)[scope.scope_name]
    return f"{scope.scope_name} ({_format_scope_value(body)})" if body else scope.scope_name


def _format_scope_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(_format_scope_value(item) for item in value)
    if isinstance(value, dict):
        return "; ".join(f"{key}: {_format_scope_value(item)}" for key, item in value.items())
    return str(value)


def _redundant_message(kind: str, name: str, entry: _AclEntry, covered_actions: list[str]) -> str:
    acl = f"{_article(entry.label)} {entry.label}"
    scope_text = _format_scope(entry.scope)
    if covered_actions == entry.actions:
        return (
            f"{kind} '{name}' has {acl} with {_join_words(entry.actions)}, "
            f"scoped to {scope_text}, that is already covered by the group's other {entry.label} entries."
        )
    verb = "is" if len(covered_actions) == 1 else "are"
    return (
        f"{kind} '{name}' has {acl} whose {_actions_phrase(covered_actions)}, "
        f"scoped to {scope_text}, {verb} already covered by the group's other {entry.label} entries."
    )


def _redundant_fix(entry: _AclEntry, covered_actions: list[str]) -> str:
    if covered_actions == entry.actions:
        return f"Remove this {entry.label}."
    return f"Remove the {_actions_phrase(covered_actions)} from this {entry.label}."
