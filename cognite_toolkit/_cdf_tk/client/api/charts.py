from collections.abc import Sequence
from typing import overload

from cognite.client import ClientConfig, CogniteClient
from cognite.client._api_client import APIClient
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.charts import (
    Chart,
    ChartList,
    ChartWrite,
    Visibility,
)


class ChartsAPI(APIClient):
    _RESOURCE_PATH = "/storage/charts/charts/"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self._CREATE_LIMIT = 1000
        self._RETRIEVE_LIMIT = 1000
        self._DELETE_LIMIT = 1000
        self._LIST_LIMIT = 1000

    @overload
    def create(self, items: ChartWrite) -> Chart: ...

    @overload
    def create(self, items: Sequence[ChartWrite]) -> ChartList: ...

    def create(self, items: ChartWrite | Sequence[ChartWrite]) -> Chart | ChartList:
        """Create one or more charts in CDF.

        Args:
            items (ChartWrite | Sequence[ChartWrite]): Chart(s) to create.

        Returns:
            ChartList: List of created charts.
        """
        if isinstance(items, Sequence) and len(items) > self._CREATE_LIMIT:
            raise ValueError(f"Cannot create more than {self._CREATE_LIMIT} charts at a time.")
        body = {
            "items": [item.as_write().dump() for item in items]
            if isinstance(items, Sequence)
            else items.as_write().dump()
        }
        response = self._put(
            url_path=self._RESOURCE_PATH,
            json=body,
        )
        if isinstance(items, Sequence):
            return ChartList._load(response.json(), cognite_client=self._cognite_client)
        elif isinstance(items, ChartWrite):
            return Chart._load(response.json(), cognite_client=self._cognite_client)
        else:
            raise ValueError("Invalid type for items. Must be ChartWrite or Sequence[ChartWrite].")

    @overload
    def retrieve(self, external_id: str) -> Chart: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> ChartList: ...

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> Chart | ChartList:
        """Retrieve one or more charts by their external ID.

        Args:
            external_id (str | Sequence[str]): External ID(s) of the chart(s) to retrieve.

        Returns:
            ChartList: List of retrieved charts.
        """
        return self._retrieve_multiple(
            list_cls=ChartList, resource_cls=Chart, identifiers=IdentifierSequence.load(external_ids=external_id)
        )

    def delete(self, external_id: str | Sequence[str]) -> None:
        """Delete one or more charts by their external ID.

        Args:
            external_id (str | Sequence[str]): External ID(s) of the chart(s) to delete.
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
        )

    def list(self, visibility: Visibility | None = None, is_owned: bool = True, limit: int = 25) -> ChartList:
        """List charts based on visibility and ownership.

        Args:
            visibility (Visibility): Visibility of the charts to list.
            is_owned (bool): Whether to list only owned charts.
            limit (int, optional): Maximum number of charts to return. Defaults to 25.

        Returns:
            ChartList: List of charts matching the criteria.
        """
        filter_: dict[str, object] = {"isOwned": is_owned}
        if visibility is not None:
            filter_["visibility"] = visibility.upper()

        return self._list(
            method="POST",
            list_cls=ChartList,
            resource_cls=Chart,
            filter=filter_,
            limit=limit,
        )
