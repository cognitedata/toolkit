from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability

from cognite_toolkit._cdf_tk.client.identifiers import SignalSinkId
from cognite_toolkit._cdf_tk.client.resource_classes.signal_sink import SignalSinkRequest, SignalSinkResponse
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
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
    def get_required_capability(
        cls, items: Sequence[SignalSinkRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        # subscribeSignalsAcl is not yet in the SDK — return empty to skip capability verification.
        return []

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
