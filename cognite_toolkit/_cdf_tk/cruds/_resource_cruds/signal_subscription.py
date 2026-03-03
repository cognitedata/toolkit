from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability

from cognite_toolkit._cdf_tk.client.identifiers import SignalSubscriptionId
from cognite_toolkit._cdf_tk.client.resource_classes.signal_subscription import (
    SignalSubscriptionRequest,
    SignalSubscriptionResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.yaml_classes import SignalSubscriptionYAML


@final
class SignalSubscriptionCRUD(ResourceCRUD[SignalSubscriptionId, SignalSubscriptionRequest, SignalSubscriptionResponse]):
    folder_name = "signals"
    resource_cls = SignalSubscriptionResponse
    resource_write_cls = SignalSubscriptionRequest
    kind = "Subscription"
    yaml_cls = SignalSubscriptionYAML
    dependencies = frozenset()
    support_update = True
    _doc_url = "Signals/operation/createSignalSubscriptions"

    @property
    def display_name(self) -> str:
        return "signal subscriptions"

    @classmethod
    def get_id(cls, item: SignalSubscriptionRequest | SignalSubscriptionResponse | dict) -> SignalSubscriptionId:
        if isinstance(item, dict):
            return SignalSubscriptionId(external_id=item["externalId"])
        return SignalSubscriptionId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: SignalSubscriptionId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SignalSubscriptionRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        return []

    def create(self, items: Sequence[SignalSubscriptionRequest]) -> list[SignalSubscriptionResponse]:
        return self.client.tool.signal_subscriptions.create(list(items))

    def retrieve(self, ids: Sequence[SignalSubscriptionId]) -> list[SignalSubscriptionResponse]:
        return []

    def update(self, items: Sequence[SignalSubscriptionRequest]) -> list[SignalSubscriptionResponse]:
        return self.client.tool.signal_subscriptions.update(list(items))

    def delete(self, ids: Sequence[SignalSubscriptionId]) -> int:
        self.client.tool.signal_subscriptions.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[SignalSubscriptionResponse]:
        if data_set_external_id or space or parent_ids:
            return iter([])
        return iter(self.client.tool.signal_subscriptions.list())
