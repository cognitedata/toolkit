from collections.abc import Sequence
from typing import Any, overload

from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client.data_classes.data_modeling import NodeId
from cognite.client.utils._auxiliary import exactly_one_is_not_none
from cognite.client.utils._identifier import IdentifierSequence
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeries, ExtendedTimeSeriesList
from cognite_toolkit._cdf_tk.client.data_classes.pending_instance_id import PendingIdentifier


class ExtendedTimeSeriesAPI(TimeSeriesAPI):
    """Extended TimeSeriesAPI to include pending ID methods."""

    @overload
    def set_pending_ids(self, instance_id: Sequence[PendingIdentifier]) -> ExtendedTimeSeriesList: ...

    @overload
    def set_pending_ids(
        self, instance_id: NodeId | tuple[str, str], id: int | None = None, external_id: str | None = None
    ) -> ExtendedTimeSeries: ...

    def set_pending_ids(
        self,
        instance_id: NodeId | tuple[str, str] | Sequence[PendingIdentifier],
        id: int | None = None,
        external_id: str | None = None,
    ) -> ExtendedTimeSeries | ExtendedTimeSeriesList:
        """Set a pending identifier for a time series.

        Args:
            instance_id: The pending instance ID to set.
            id: The ID of the time series.
            external_id: The external ID of the time series.

        Returns:
            PendingIdentifier: An object containing the pending identifier information.
        """
        if isinstance(instance_id, NodeId) or (
            isinstance(instance_id, tuple)
            and len(instance_id) == 2
            and isinstance(instance_id[0], str)
            and isinstance(instance_id[1], str)
        ):
            return self._set_single_pending_id(instance_id, id, external_id)
        elif isinstance(instance_id, Sequence) and all(isinstance(item, PendingIdentifier) for item in instance_id):
            return self._set_multiple_pending_ids(instance_id)  # type: ignore[arg-type]
        else:
            raise TypeError(
                "instance_id must be a NodeId, a tuple of (str, str), or a sequence of PendingIdentifier objects."
            )

    def _set_single_pending_id(
        self, instance_id: NodeId | tuple[str, str], id: int | None = None, external_id: str | None = None
    ) -> ExtendedTimeSeries:
        if not exactly_one_is_not_none(id, external_id):
            raise ValueError("Exactly one of 'id' or 'external_id' must be provided.")
        body: dict[str, Any] = {
            "pendingInstanceId": NodeId.load(instance_id).dump(include_instance_type=False),
        }
        if id is not None:
            body["id"] = id
        if external_id is not None:
            body["externalId"] = external_id

        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/set-pending-instance-ids",
            json={"items": [body]},
            headers={"cdf-version": "alpha"},
        )
        data = response.json()
        if "items" not in data or not data["items"]:
            raise ValueError("No items returned from the API. Check if the request was successful.")

        return ExtendedTimeSeries._load(data["items"][0], cognite_client=self._cognite_client)

    def _set_multiple_pending_ids(self, identifiers: Sequence[PendingIdentifier]) -> ExtendedTimeSeriesList:
        """Set multiple pending identifiers for time series.

        Args:
            identifiers: A sequence of PendingIdentifier objects containing the pending instance IDs and optional IDs or external IDs.

        Returns:
            ExtendedTimeSeriesList: A list of ExtendedTimeSeries objects with the updated pending identifiers.
        """
        body = [identifier.dump(camel_case=False) for identifier in identifiers]
        response = self._post(
            url_path=f"{self._RESOURCE_PATH}/set-pending-instance-ids", json={"items": body}, api_subversion="alpha"
        )
        data = response.json()
        return ExtendedTimeSeriesList._load(data["items"], cognite_client=self._cognite_client)

    def retrieve(
        self, id: int | None = None, external_id: str | None = None, instance_id: NodeId | None = None
    ) -> ExtendedTimeSeries | None:
        """`Retrieve a single time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            id (int | None): ID
            external_id (str | None): External ID
            instance_id (NodeId | None): Instance ID

        Returns:
            TimeSeries | None: Requested time series or None if it does not exist.

        Examples:

            Get time series by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve(id=1)

            Get time series by external id:

                >>> res = client.time_series.retrieve(external_id="1")
        """
        identifiers = IdentifierSequence.load(ids=id, external_ids=external_id, instance_ids=instance_id).as_singleton()
        return self._retrieve_multiple(
            list_cls=ExtendedTimeSeriesList,
            resource_cls=ExtendedTimeSeries,
            identifiers=identifiers,
            api_subversion="alpha",
        )

    def retrieve_multiple(
        self,
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtendedTimeSeriesList:
        """`Retrieve multiple time series by id. <https://developer.cognite.com/api#tag/Time-series/operation/getTimeSeriesByIds>`_

        Args:
            ids (Sequence[int] | None): IDs
            external_ids (SequenceNotStr[str] | None): External IDs
            instance_ids (Sequence[NodeId] | None): Instance IDs
            ignore_unknown_ids (bool): Ignore IDs and external IDs that are not found rather than throw an exception.

        Returns:
            TimeSeriesList: The requested time series.

        Examples:

            Get time series by id:

                >>> from cognite.client import CogniteClient
                >>> client = CogniteClient()
                >>> res = client.time_series.retrieve_multiple(ids=[1, 2, 3])

            Get time series by external id:

                >>> res = client.time_series.retrieve_multiple(external_ids=["abc", "def"])
        """
        identifiers = IdentifierSequence.load(ids=ids, external_ids=external_ids, instance_ids=instance_ids)
        return self._retrieve_multiple(
            list_cls=ExtendedTimeSeriesList,
            resource_cls=ExtendedTimeSeries,
            identifiers=identifiers,
            ignore_unknown_ids=ignore_unknown_ids,
            api_subversion="alpha",
        )
