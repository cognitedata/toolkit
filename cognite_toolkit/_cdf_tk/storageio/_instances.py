from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Literal

from cognite.client.data_classes.aggregations import Count
from cognite.client.utils._identifier import InstanceId
from rich.console import Console

from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList, InstanceList
from cognite_toolkit._cdf_tk.utils.cdf import iterate_instances
from cognite_toolkit._cdf_tk.utils.fileio import SchemaColumn
from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient, HTTPMessage, ItemsRequest
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal
from cognite_toolkit._cdf_tk.utils.validate_access import ValidateAccess

from ._base import StorageIOConfig, T_Selector, TableStorageIO
from ._selectors import InstanceSelector, InstanceViewSelector


class InstanceIO(TableStorageIO[InstanceId, InstanceSelector, InstanceApplyList, InstanceList]):
    folder_name = "instances"
    kind = "Instances"
    display_name = "Instances"
    supported_download_formats = frozenset({".parquet", ".csv", ".ndjson"})
    supported_compressions = frozenset({".gz"})
    supported_read_formats = frozenset({".parquet", ".csv", ".ndjson", ".yaml", ".yml"})
    chunk_size = 1000

    def as_id(self, item: dict[str, JsonVal] | object) -> InstanceId:
        if isinstance(item, dict) and "externalId" in item and "space" in item:
            return InstanceId(space=item["space"], external_id=item["externalId"])  # type: ignore[arg-type]
        if isinstance(item, InstanceId):
            return item
        raise TypeError(f"Cannot extract ID from item of type {type(item).__name__!r}")

    def _validate_auth(
        self, actions: Sequence[Literal["read", "write"]], selector: InstanceSelector, validator: ValidateAccess
    ) -> None:
        if schema_spaces := selector.get_schema_spaces():
            validator.data_model(actions, set(schema_spaces))

        if instance_spaces := selector.get_instance_spaces():
            validator.data_model(actions, set(instance_spaces))

    def download_iterable(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[InstanceList]:
        if isinstance(selector, InstanceViewSelector):
            chunk = InstanceList([])
            total = 0
            for instance in iterate_instances(
                client=self.client,
                source=selector.view,
                instance_type=selector.instance_type,
                space=list(selector.instance_spaces) if selector.instance_spaces else None,
            ):
                if limit is not None and total >= limit:
                    break
                total += 1
                chunk.append(instance)
                if len(chunk) >= self.chunk_size:
                    yield chunk
                    chunk = InstanceList([])
            if chunk:
                yield chunk
        else:
            raise NotImplementedError()

    def download_ids(self, selector: InstanceSelector, limit: int | None = None) -> Iterable[list[InstanceId]]:
        if isinstance(selector, InstanceViewSelector):
            yield from ([instance.as_id() for instance in chunk] for chunk in self.download_iterable(selector, limit))  # type: ignore[attr-defined]
        else:
            raise NotImplementedError()

    def count(self, selector: InstanceSelector) -> int | None:
        if isinstance(selector, InstanceViewSelector):
            result = self.client.data_modeling.instances.aggregate(
                view=selector.view,
                aggregates=Count("externalId"),
                instance_type=selector.instance_type,
                space=list(selector.instance_spaces) if selector.instance_spaces else None,
            )
            return int(result.value or 0)
        raise NotImplementedError()

    def upload_items(self, data_chunk: InstanceApplyList, selector: InstanceSelector) -> None:
        raise NotImplementedError()

    def upload_items_force(
        self, data_chunk: InstanceApplyList, http_client: HTTPClient, selector: T_Selector | None = None
    ) -> Sequence[HTTPMessage]:
        config = self.client.config
        # MyPy fails to understand that ResponseMessage | FailedRequestMessage are both subclasses of HTTPMessage.
        return http_client.request_with_retries(  # type: ignore[return-value]
            ItemsRequest(
                endpoint_url=config.create_api_url("/models/instances"),
                method="POST",
                items=data_chunk.dump(camel_case=True),  # type: ignore[arg-type]
                extra_body_fields={"autoCreateDirectRelations": True},
                as_id=self.as_id,
            )
        )

    def data_to_json_chunk(self, data_chunk: InstanceList) -> list[dict[str, JsonVal]]:
        raise NotImplementedError()

    def json_chunk_to_data(self, data_chunk: list[dict[str, JsonVal]]) -> InstanceApplyList:
        raise NotImplementedError()

    def configurations(self, selector: InstanceSelector) -> Iterable[StorageIOConfig]:
        raise NotImplementedError()

    def load_selector(self, datafile: Path) -> InstanceSelector:
        raise NotImplementedError()

    def ensure_configurations(self, selector: InstanceSelector, console: Console | None = None) -> None:
        raise NotImplementedError()

    def get_schema(self, selector: InstanceSelector) -> list[SchemaColumn]:
        raise NotImplementedError()
