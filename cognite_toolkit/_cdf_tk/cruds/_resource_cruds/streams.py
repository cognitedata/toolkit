from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability, StreamsAcl
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.streams import (
    StreamRequest,
    StreamResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import StreamYAML

from .datamodel import ContainerCRUD


@final
class StreamCRUD(ResourceCRUD[ExternalId, StreamRequest, StreamResponse]):
    folder_name = "streams"
    resource_cls = StreamResponse
    resource_write_cls = StreamRequest
    kind = "Streams"
    yaml_cls = StreamYAML
    dependencies = frozenset({ContainerCRUD})
    _doc_url = "Streams/operation/createStream"
    support_update = False

    @property
    def display_name(self) -> str:
        return "streams"

    @classmethod
    def get_id(cls, item: StreamRequest | StreamResponse | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_required_capability(
        cls, items: Sequence[StreamRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [StreamsAcl.Action.Read]
            if read_only
            else [StreamsAcl.Action.Read, StreamsAcl.Action.Create, StreamsAcl.Action.Delete]
        )
        return StreamsAcl(actions, StreamsAcl.Scope.All())

    def create(self, items: Sequence[StreamRequest]) -> list[StreamResponse]:
        return self.client.streams.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[StreamResponse]:
        return self.client.streams.retrieve(ids, ignore_unknown_ids=True)

    def delete(self, ids: SequenceNotStr[ExternalId]) -> int:
        count = 0
        for id_ in ids:
            try:
                self.client.streams.delete([id_])
            except ToolkitAPIError:
                continue
            count += 1
        return count

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[StreamResponse]:
        if data_set_external_id or space or parent_ids:
            # These filters are not supported for Streams
            return iter([])

        all_streams = self.client.streams.list()
        return iter(all_streams)
