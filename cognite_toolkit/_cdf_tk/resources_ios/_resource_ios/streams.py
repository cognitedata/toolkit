import time
from collections.abc import Hashable, Iterable, Sequence
from datetime import timedelta
from typing import Any, Literal, final

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    ScopeDefinition,
    StreamsAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.streams import (
    StreamRequest,
    StreamResponse,
)
from cognite_toolkit._cdf_tk.resources_ios._base_cruds import ResourceIO
from cognite_toolkit._cdf_tk.utils.time import time_windows_ms
from cognite_toolkit._cdf_tk.yaml_classes import StreamYAML

from .datamodel import ContainerCRUD

_TIMEDELTA_ADAPTER: TypeAdapter[timedelta] = TypeAdapter(timedelta)


@final
class StreamIO(ResourceIO[ExternalId, StreamRequest, StreamResponse]):
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
    def get_id(cls, item: StreamRequest | StreamResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_minimum_scope(cls, items: Sequence[StreamRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            acl_actions: list[Literal["READ", "CREATE", "DELETE"]] = []
            if "READ" in actions:
                acl_actions.append("READ")
            if "WRITE" in actions:
                acl_actions.extend(["CREATE", "DELETE"])
            yield StreamsAcl(actions=acl_actions, scope=scope)

    def create(self, items: Sequence[StreamRequest]) -> list[StreamResponse]:
        return self.client.streams.create(items)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[StreamResponse]:
        return self.client.streams.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        self.client.streams.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[StreamResponse]:
        if data_set_external_id or space or parent_ids:
            # These filters are not supported for Streams
            return iter([])

        all_streams = self.client.streams.list()
        return iter(all_streams)

    def last_updated_time_windows(
        self, stream_external_id: str, start_ms: int | None = None
    ) -> list[dict[str, int] | None]:
        """Return lastUpdatedTime filter dicts to use in record queries.

        Each dict is {"gte": ..., "lt": ...} representing one query window.
        None is returned for Mutable streams with no start_ms — meaning no time filter is needed.
        Returns an empty list if the stream does not exist.

        Immutable streams enforce a maxFilteringInterval per request, so the range
        [start_ms (or stream.createdTime), now) is split into consecutive windows.
        Mutable streams have no such constraint and are covered in a single pass.
        """
        streams = self.retrieve(ExternalId.from_external_ids([stream_external_id]))
        if not streams:
            return []
        stream = streams[0]
        now_ms = int(time.time() * 1000)
        if stream.type == "Mutable":
            if start_ms is None:
                return [None]
            return [{"gte": start_ms, "lt": now_ms}]
        effective_start_ms = start_ms if start_ms is not None else stream.created_time
        max_interval_ms: int | None = None
        if stream.settings and stream.settings.limits.max_filtering_interval:
            td = _TIMEDELTA_ADAPTER.validate_python(stream.settings.limits.max_filtering_interval)
            max_interval_ms = int(td.total_seconds() * 1000)
        return [
            {"gte": window_start, "lt": window_end}
            for window_start, window_end in time_windows_ms(effective_start_ms, now_ms, max_interval_ms)
        ]
