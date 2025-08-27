from collections.abc import Sequence
from typing import overload
from urllib.parse import urljoin

from cognite.client._api_client import APIClient
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.charts import (
    Chart,
    ChartList,
    ChartWrite,
    Visibility,
)
from cognite_toolkit._cdf_tk.utils.collection import chunker


class ChartsAPI(APIClient):
    _RESOURCE_PATH = "/storage/charts/charts"

    def _get_base_url_with_base_path(self) -> str:
        """
        This method is overridden to provide the correct base path for the Search Configurations API.
        This method in base class APIClient appends /api/{api_version}/ to the base URL,
        but for Charts API, we need a different path structure.
        """
        base_path = ""
        if self._api_version:
            base_path = f"/apps/{self._api_version}/projects/{self._config.project}"
        return urljoin(self._config.base_url, base_path)

    @overload
    def upsert(self, items: ChartWrite) -> Chart: ...

    @overload
    def upsert(self, items: Sequence[ChartWrite]) -> ChartList: ...

    def upsert(self, items: ChartWrite | Sequence[ChartWrite]) -> Chart | ChartList:
        """Upsert one or more charts in CDF.

        Args:
            items (ChartWrite | Sequence[ChartWrite]): Chart(s) to upsert. Can be a single ChartWrite or a sequence of ChartWrite.

        Returns:
            Chart | ChartList: List of upserted charts if multiple items are provided, otherwise a single Chart.
        """
        item_sequence = items if isinstance(items, Sequence) else [items]
        result = ChartList([])
        # We are avoiding concurrency here as the Charts backend it not necessarily designed for it.
        for chunk in chunker(item_sequence, self._CREATE_LIMIT):
            body = {"items": [chunk_item.as_write().dump() for chunk_item in chunk]}
            response = self._put(
                url_path=self._RESOURCE_PATH,
                json=body,
            )
            result.extend([Chart._load(item, cognite_client=self._cognite_client) for item in response.json()["items"]])

        if not result:
            raise ValueError("No charts were upserted. This may indicate an issue with the upsert endpoint.")

        if isinstance(items, ChartWrite):
            if len(result) != 1:
                raise ValueError(
                    "Expected a single chart to be returned, but multiple charts were returned. "
                    "This may indicate an issue with the upsert operation."
                )
            return result[0]
        elif len(result) != len(items):
            raise ValueError(
                "The number of upserted charts does not match the number of input charts. "
                "This may indicate an issue with the upsert endpoint."
            )
        return result

    @overload
    def retrieve(self, external_id: str) -> Chart: ...

    @overload
    def retrieve(self, external_id: SequenceNotStr[str]) -> ChartList: ...

    def retrieve(self, external_id: str | SequenceNotStr[str]) -> Chart | ChartList:
        """Retrieve one or more charts by their external ID.

        Args:
            external_id (str | Sequence[str]): External ID(s) of the chart(s) to retrieve.

        Returns:
            Chart | ChartList: A single Chart if a single external ID is provided, otherwise a ChartList.
        """
        return self._retrieve_multiple(
            list_cls=ChartList, resource_cls=Chart, identifiers=IdentifierSequence.load(external_ids=external_id)
        )

    def delete(self, external_id: str | SequenceNotStr[str]) -> None:
        """Delete one or more charts by their external ID.

        Args:
            external_id (str | Sequence[str]): External ID(s) of the chart(s) to delete.
        """
        self._delete_multiple(
            identifiers=IdentifierSequence.load(external_ids=external_id),
            wrap_ids=True,
        )

    def list(self, visibility: Visibility | None = None, is_owned: bool | None = None) -> ChartList:
        """List charts based on visibility and ownership.

        Args:
            visibility (Visibility): Visibility of the charts to list, either 'PUBLIC' or 'PRIVATE'.
            is_owned (bool): Whether to list only owned charts.

        Returns:
            ChartList: List of charts matching the criteria.
        """
        filter_: dict[str, str | bool] = {}
        if visibility is not None:
            filter_["visibility"] = visibility.upper()
        if is_owned is not None:
            filter_["isOwned"] = is_owned

        response = self._post(
            url_path=self._RESOURCE_PATH + "/list",
            json={"filter": filter_} if filter_ else {},
        )
        return ChartList._load(response.json()["items"], cognite_client=self._cognite_client)
