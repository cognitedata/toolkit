from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import SignalSinkId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    CurrentUserScope,
    ScopeDefinition,
    SubscribeSignalsAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.signal_sink import SignalSinkRequest, SignalSinkResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning, MediumSeverityWarning
from cognite_toolkit._cdf_tk.yaml_classes import SignalSinkYAML


@final
class SignalSinkCRUD(ResourceCRUD[SignalSinkId, SignalSinkRequest, SignalSinkResponse]):
    folder_name = "signals"
    resource_cls = SignalSinkResponse
    resource_write_cls = SignalSinkRequest
    kind = "Sink"
    yaml_cls = SignalSinkYAML
    dependencies = frozenset()
    support_update = True
    _doc_url = "Signals/operation/createSignalSinks"

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None = None) -> None:
        super().__init__(client, build_dir, console)
        self._known_emails: set[str] | None = None

    @property
    def display_name(self) -> str:
        return "signal sinks"

    @classmethod
    def get_id(cls, item: SignalSinkRequest | SignalSinkResponse | dict) -> SignalSinkId:
        if isinstance(item, dict):
            return SignalSinkId(type=item["type"], external_id=item["externalId"])
        return SignalSinkId(type=item.type, external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: SignalSinkId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_minimum_scope(cls, items: Sequence[SignalSinkRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope | CurrentUserScope):
            yield SubscribeSignalsAcl(actions=sorted(actions), scope=scope)

    def _get_known_emails(self) -> set[str]:
        if self._known_emails is None:
            try:
                profiles = self.client.user_profiles.list(limit=None)
                self._known_emails = {p.email.casefold() for p in profiles if p.email}
            except ToolkitAPIError as e:
                if e.code in (401, 403):
                    LowSeverityWarning(
                        "Could not fetch user profiles to validate email addresses: access denied."
                    ).print_warning(console=self.console)
                else:
                    raise
                self._known_emails = set()
        return self._known_emails

    def _warn_if_email_unknown(self, resource: SignalSinkRequest) -> None:
        if resource.type != "email" or not resource.email_address:
            return
        known = self._get_known_emails()
        if not known:
            return
        if resource.email_address.casefold() not in known:
            MediumSeverityWarning(
                f"Email address {resource.email_address!r} in sink {resource.external_id!r} "
                f"does not match any known user profile."
            ).print_warning(console=self.console)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SignalSinkRequest:
        loaded = super().load_resource(resource, is_dry_run)
        self._warn_if_email_unknown(loaded)
        return loaded

    def create(self, items: Sequence[SignalSinkRequest]) -> list[SignalSinkResponse]:
        return self.client.tool.signal_sinks.create(list(items))

    def retrieve(self, ids: Sequence[SignalSinkId]) -> list[SignalSinkResponse]:
        return self.client.tool.signal_sinks.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[SignalSinkRequest]) -> list[SignalSinkResponse]:
        return self.client.tool.signal_sinks.update(list(items))

    def delete(self, ids: Sequence[SignalSinkId]) -> int:
        self.client.tool.signal_sinks.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SignalSinkResponse]:
        if data_set_external_id or space or parent_ids:
            return iter([])
        return iter(self.client.tool.signal_sinks.list())
