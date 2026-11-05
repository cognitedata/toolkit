from collections import defaultdict
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self, cast, get_args, get_origin

from pydantic import Discriminator, Field, Tag, ValidationError, field_validator, model_validator
from pydantic.functional_validators import BeforeValidator

from cognite_toolkit._cdf_tk.client.identifiers import NameId
from cognite_toolkit._cdf_tk.client.resource_classes.group import GroupAttributes
from cognite_toolkit._cdf_tk.client.resource_classes.group.scope_logic import scope_difference, scope_union
from cognite_toolkit._cdf_tk.client.resource_classes.group.scopes import AllScope as ClientAllScope
from cognite_toolkit._cdf_tk.client.resource_classes.group.scopes import Scope as ClientScope
from cognite_toolkit._cdf_tk.client.resource_classes.group.scopes import ScopeDefinition
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

from .base import ToolkitResource
from .capabilities import CapabilityType, Scope, UnknownCapability

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildWarning, Recommendation
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
        insights: list[BuildWarning | Recommendation] = []
        insights.extend(self._unrecognize_capabilities(source_file, resource_type))
        if Flags.ALPHA_RULES.is_enabled():
            insights.extend(self._redundant_capability_recommendations(source_file, resource_type))
        return insights

    def _unrecognize_capabilities(self, source_file: Path, resource_type: "ResourceType") -> "list[BuildWarning]":
        """Return warnings for capabilities that are not recognized by the toolkit."""
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildWarning

        unrecognized = [
            BuildWarning(
                code="UNRECOGNIZED-VALUE",
                message=(
                    f"'{cap.original_name}' is not a known capability name. "
                    "It will be deployed as-is, but may be rejected by CDF."
                ),
                source_file=source_file,
                fix="Compare the YAML with reference documentation. The resource will still be deployed.",
            )
            for cap in (self.capabilities or [])
            if isinstance(cap, UnknownCapability)
        ]
        return unrecognized

    def _redundant_capability_recommendations(
        self, source_file: Path, resource_type: "ResourceType"
    ) -> "list[Recommendation]":
        """Recommend dropping ACLs whose actions and scope are already granted by other ACLs.

        An ACL is redundant when every action it grants is contained in the scopes of the group's
        other ACLs of the same type. A space-scoped ``dataModelInstancesAcl`` is redundant when
        another ``dataModelInstancesAcl`` grants the same actions on ``all``.
        """
        # Lazy import to avoid circular dependency (yaml_classes → commands.build_v2 → resource_ios → yaml_classes).
        from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation

        grouped: dict[str, list[_AclEntry]] = defaultdict(list)
        encoder: dict[str, int] = {}
        for position, capability in enumerate(self.capabilities or []):
            # Unknown ACLs are preserved as-is. Their actions are not modeled, so they are not
            # compared and they do not count as covering another ACL.
            if isinstance(capability, UnknownCapability):
                continue
            actions = _capability_actions(capability)
            client_scope = _to_client_scope(capability.scope, encoder)
            if not actions or client_scope is None:
                continue
            label = capability.capability_name
            grouped[label].append(
                _AclEntry(
                    position=position,
                    label=label,
                    actions=actions,
                    scope=client_scope,
                    scope_text=_format_scope(capability.scope),
                )
            )

        findings: list[tuple[int, Recommendation]] = []
        kind = resource_type.kind
        for entries in grouped.values():
            for entry, covered_actions in _select_redundant(entries):
                whole_acl = len(covered_actions) == len(entry.actions)
                findings.append(
                    (
                        entry.position,
                        Recommendation(
                            code="REDUNDANT-PROPERTY",
                            message=_redundant_message(kind, self.name, entry, covered_actions, whole_acl=whole_acl),
                            fix=_redundant_fix(entry.label, covered_actions, whole_acl=whole_acl),
                            source_file=source_file,
                            alpha=Flags.ALPHA_RULES.is_enabled(),
                        ),
                    )
                )
        findings.sort(key=lambda item: item[0])
        return [recommendation for _, recommendation in findings]


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


@dataclass(frozen=True)
class _AclEntry:
    position: int
    label: str
    actions: list[str]
    scope: ClientScope
    scope_text: str


def _capability_actions(capability: CapabilityType) -> list[str]:
    raw_actions = getattr(capability, "actions", None)
    if not isinstance(raw_actions, list):
        return []
    seen: set[str] = set()
    actions: list[str] = []
    for action in raw_actions:
        if isinstance(action, str) and action not in seen:
            seen.add(action)
            actions.append(action)
    return actions


@lru_cache(maxsize=1)
def _client_scopes_by_name() -> dict[str, type[ScopeDefinition]]:
    mapping: dict[str, type[ScopeDefinition]] = {}
    for cls in get_concrete_subclasses(ScopeDefinition):
        default = cls.model_fields["scope_name"].default
        if isinstance(default, str):
            mapping[default] = cls
    return mapping


def _is_int_list(annotation: Any) -> bool:
    if get_origin(annotation) is not list:
        return False
    args = get_args(annotation)
    return len(args) == 1 and args[0] is int


def _encode_scope_id(value: object, encoder: dict[str, int]) -> int:
    """Map a toolkit scope id onto an int the client scope model accepts.

    Group YAML keeps ids as strings, because they are often external ids replaced at deploy time.
    The same string always maps to the same int within one group, so scope comparisons stay exact.
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    key = str(value)
    code = encoder.get(key)
    if code is None:
        # Negative codes cannot collide with real CDF internal ids, which are positive.
        code = -len(encoder) - 1
        encoder[key] = code
    return code


def _to_client_scope(scope: Scope, encoder: dict[str, int]) -> ClientScope | None:
    """Convert a YAML scope into the client scope used by ``scope_logic``.

    Returns None when the scope has no client equivalent (for example an unknown scope). Those
    ACLs are skipped: they are not reported as redundant, and they are not treated as covering others.
    """
    client_cls = _client_scopes_by_name().get(scope.scope_name)
    if client_cls is None:
        return None
    data: dict[str, Any] = {}
    for name, field in client_cls.model_fields.items():
        if name == "scope_name" or not hasattr(scope, name):
            continue
        value = getattr(scope, name)
        if _is_int_list(field.annotation) and isinstance(value, list):
            data[name] = [_encode_scope_id(item, encoder) for item in value]
        else:
            data[name] = value
    try:
        return cast(ClientScope, client_cls.model_validate(data))
    except ValidationError:
        return None


def _scope_covered(scope: ClientScope, others: list[ClientScope]) -> bool:
    """Return whether ``scope`` is contained in the union of ``others``."""
    if any(isinstance(other, ClientAllScope) for other in others):
        return True
    if isinstance(scope, ClientAllScope):
        return False
    same_type = [other for other in others if type(other) is type(scope)]
    if not same_type:
        return False
    try:
        return scope_difference(scope, scope_union(*same_type)) is None
    except (TypeError, ValueError):
        return False


def _covered_actions(entry: _AclEntry, others: list[_AclEntry]) -> list[str]:
    covered: list[str] = []
    for action in entry.actions:
        scopes = [other.scope for other in others if action in other.actions]
        if scopes and _scope_covered(entry.scope, scopes):
            covered.append(action)
    return covered


def _select_redundant(entries: list[_AclEntry]) -> list[tuple[_AclEntry, list[str]]]:
    """Pick ACLs that can be removed without changing the access the group grants.

    Whole ACLs are removed first, latest first, so an exact duplicate keeps the earliest entry.
    Actions covered only in part are reported on the ACLs that remain.
    """
    kept = set(range(len(entries)))
    whole: list[tuple[_AclEntry, list[str]]] = []
    for index in reversed(range(len(entries))):
        others = [entries[other] for other in kept if other != index]
        covered = _covered_actions(entries[index], others)
        if covered and len(covered) == len(entries[index].actions):
            kept.remove(index)
            whole.append((entries[index], covered))

    partial: list[tuple[_AclEntry, list[str]]] = []
    for index in kept:
        others = [entries[other] for other in kept if other != index]
        covered = _covered_actions(entries[index], others)
        if covered and len(covered) < len(entries[index].actions):
            partial.append((entries[index], covered))
    return sorted([*whole, *partial], key=lambda item: item[0].position)


def _join_words(items: list[str]) -> str:
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])}, and {items[-1]}"


def _article(word: str) -> str:
    return "an" if word[:1].lower() in {"a", "e", "i", "o", "u"} else "a"


def _format_scope(scope: Scope) -> str:
    dumped = scope.model_dump(by_alias=True)
    if not isinstance(dumped, dict) or not dumped:
        return scope.scope_name
    scope_name, body = next(iter(dumped.items()))
    if not isinstance(body, dict) or not body:
        return str(scope_name)
    return f"{scope_name} ({_format_scope_body(body)})"


def _format_scope_body(body: dict[str, Any]) -> str:
    return "; ".join(f"{key}: {_format_scope_value(value)}" for key, value in body.items())


def _format_scope_value(value: Any) -> str:
    if isinstance(value, list):
        return ", ".join(_format_scope_value(item) for item in value)
    if isinstance(value, dict):
        return "; ".join(f"{key}: {_format_scope_value(item)}" for key, item in value.items())
    return str(value)


def _redundant_message(kind: str, name: str, entry: _AclEntry, covered_actions: list[str], *, whole_acl: bool) -> str:
    article = _article(entry.label)
    if whole_acl:
        return (
            f"{kind} '{name}' has {article} {entry.label} with {_join_words(entry.actions)}, "
            f"scoped to {entry.scope_text}, that is already covered by the group's other {entry.label} entries."
        )
    noun = "action" if len(covered_actions) == 1 else "actions"
    verb = "is" if len(covered_actions) == 1 else "are"
    return (
        f"{kind} '{name}' has {article} {entry.label} whose {_join_words(covered_actions)} {noun}, "
        f"scoped to {entry.scope_text}, {verb} already covered by the group's other {entry.label} entries."
    )


def _redundant_fix(label: str, covered_actions: list[str], *, whole_acl: bool) -> str:
    if whole_acl:
        return f"Remove this {label}."
    noun = "action" if len(covered_actions) == 1 else "actions"
    return f"Remove the {_join_words(covered_actions)} {noun} from this {label}."
