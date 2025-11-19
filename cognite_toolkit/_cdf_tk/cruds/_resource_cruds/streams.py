from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability, StreamsAcl
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.streams import (
    StreamRequest,
    StreamRequestList,
    StreamResponse,
    StreamResponseList,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import StreamYAML


@final
class StreamCRUD(ResourceCRUD[str, StreamRequest, StreamResponse, StreamRequestList, StreamResponseList]):
    folder_name = "streams"
    filename_pattern = r"^.*\.Streams$"
    filetypes = frozenset({"yaml", "yml"})
    resource_cls = StreamResponse
    resource_write_cls = StreamRequest
    list_cls = StreamResponseList
    list_write_cls = StreamRequestList
    kind = "Streams"
    yaml_cls = StreamYAML
    dependencies = frozenset()
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

    def create(self, items: StreamRequestList) -> StreamResponseList:
        created = self.client.streams.create(list(items))
        return StreamResponseList(created)

    def retrieve(self, ids: SequenceNotStr[str]) -> StreamResponseList:
        retrieved: list[StreamResponse] = []
        for _id in ids:
            try:
                _resp = self.client.streams.retrieve(_id)
                if _resp:
                    retrieved.append(_resp)
            except Exception:
                pass
        return StreamResponseList(retrieved)

    def delete(self, ids: SequenceNotStr[str]) -> int:
        count = 0
        for _id in ids:
            try:
                self.client.streams.delete(_id)
                count += 1
            except Exception:
                pass
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
