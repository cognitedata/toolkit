from collections.abc import Hashable, Iterable, Sequence
from typing import Any, Literal, final

from cognite.client.data_classes.capabilities import Capability

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, SignalSinkId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    Acl,
    AllScope,
    CurrentUserScope,
    ScopeDefinition,
    SubscribeSignalsAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    SignalSubscriptionRequest,
    SignalSubscriptionResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.hosted_extractors import (
    HostedExtractorDestinationCRUD,
    HostedExtractorSourceCRUD,
)
from cognite_toolkit._cdf_tk.cruds._resource_cruds.signal_sink import SignalSinkCRUD
from cognite_toolkit._cdf_tk.cruds._resource_cruds.workflow import WorkflowCRUD
from cognite_toolkit._cdf_tk.yaml_classes import SignalSubscriptionYAML
from cognite_toolkit._cdf_tk.yaml_classes.signal_subscription import (
    HostedExtractorsFilterYAML,
    IntegrationsFilterYAML,
    SinkRefWithExternalIdYAML,
    WorkflowsFilterYAML,
)


@final
class SignalSubscriptionCRUD(ResourceCRUD[ExternalId, SignalSubscriptionRequest, SignalSubscriptionResponse]):
    folder_name = "signals"
    resource_cls = SignalSubscriptionResponse
    resource_write_cls = SignalSubscriptionRequest
    kind = "Subscription"
    yaml_cls = SignalSubscriptionYAML
    dependencies = frozenset({SignalSinkCRUD, WorkflowCRUD, HostedExtractorDestinationCRUD, HostedExtractorSourceCRUD})
    support_update = True
    _doc_url = "Signals/operation/createSignalSubscriptions"

    @property
    def display_name(self) -> str:
        return "signal subscriptions"

    @classmethod
    def get_dependencies(cls, resource: SignalSubscriptionYAML) -> Iterable[tuple[type[ResourceCRUD], Identifier]]:
        sink = resource.sink
        if isinstance(sink, SinkRefWithExternalIdYAML):
            yield SignalSinkCRUD, SignalSinkId(type=sink.type, external_id=sink.external_id)

        if isinstance(resource.filter, IntegrationsFilterYAML) and resource.filter.resource:
            # TODO: integrations is not yet a supported resource type; add dependency once it is.
            pass
        elif isinstance(resource.filter, WorkflowsFilterYAML) and resource.filter.resource:
            yield WorkflowCRUD, ExternalId(external_id=resource.filter.resource)
        elif isinstance(resource.filter, HostedExtractorsFilterYAML):
            if resource.filter.source_external_id:
                yield HostedExtractorSourceCRUD, ExternalId(external_id=resource.filter.source_external_id)
            if resource.filter.destination_external_id:
                yield HostedExtractorDestinationCRUD, ExternalId(external_id=resource.filter.destination_external_id)

    @classmethod
    def get_id(cls, item: SignalSubscriptionRequest | SignalSubscriptionResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SignalSubscriptionRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        return []

    @classmethod
    def get_minimum_scope(cls, items: Sequence[SignalSubscriptionRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[Acl]:
        if isinstance(scope, AllScope | CurrentUserScope):
            yield SubscribeSignalsAcl(actions=sorted(actions), scope=scope)

    def create(self, items: Sequence[SignalSubscriptionRequest]) -> list[SignalSubscriptionResponse]:
        return self.client.tool.signal_subscriptions.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[SignalSubscriptionResponse]:
        id_set = {id_.external_id for id_ in ids}
        return [item for item in self.client.tool.signal_subscriptions.list() if item.external_id in id_set]

    def update(self, items: Sequence[SignalSubscriptionRequest]) -> list[SignalSubscriptionResponse]:
        return self.client.tool.signal_subscriptions.update(list(items))

    def delete(self, ids: Sequence[ExternalId]) -> int:
        self.client.tool.signal_subscriptions.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SignalSubscriptionResponse]:
        if data_set_external_id or space or parent_ids:
            return
        for page in self.client.tool.signal_subscriptions.iterate():
            yield from page
