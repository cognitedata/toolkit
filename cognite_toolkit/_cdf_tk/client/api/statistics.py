import itertools
from typing import overload

from cognite.client._api_client import APIClient
from cognite.client._cognite_client import CogniteClient
from cognite.client.config import ClientConfig
from cognite.client.data_classes.data_modeling.ids import _load_space_identifier
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.data_classes.statistics import (
    ProjectStatsAndLimits,
    SpaceInstanceCounts,
    SpaceInstanceCountsList,
)


class StatisticsAPI(APIClient):
    _RESOURCE_PATH = "/models/statistics"

    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        # This is an alpha API, which requires a specific version.
        super().__init__(config, "v1", cognite_client)
        self._RETRIEVE_LIMIT = 100

    def project(self) -> ProjectStatsAndLimits:
        """`Retrieve project-wide usage data and limits

        Returns the usage data and limits for a project's data modelling usage, including data model schemas
        and graph instances

        Returns:
            ProjectStatsAndLimits: The requested statistics and limits

        """
        response_data = self._get(self._RESOURCE_PATH).json()
        if "project" not in response_data:
            response_data["project"] = self._cognite_client._config.project
        return ProjectStatsAndLimits._load(response_data)

    @overload
    def list(self, space: str) -> SpaceInstanceCounts: ...

    @overload
    def list(self, space: SequenceNotStr[str] | None = None) -> SpaceInstanceCountsList: ...

    def list(self, space: str | SequenceNotStr[str] | None = None) -> SpaceInstanceCounts | SpaceInstanceCountsList:
        """`Retrieve usage data and limits per space

        Args:
            space (str | SequenceNotStr[str] | None): The space or spaces to retrieve statistics for.
                If None, all spaces will be retrieved.

        Returns:
            SpaceInstanceCounts | SpaceInstanceCountsList: InstanceStatsPerSpace if a single space is given, else
                InstanceStatsList (which is a list of InstanceStatsPerSpace)

        """
        if space is None:
            return SpaceInstanceCountsList._load(self._get(self._RESOURCE_PATH + "/spaces").json()["items"])

        is_single = isinstance(space, str)

        ids = _load_space_identifier(space)
        result = SpaceInstanceCountsList._load(
            itertools.chain.from_iterable(
                self._post(self._RESOURCE_PATH + "/spaces/byids", json={"items": chunk.as_dicts()}).json()["items"]
                for chunk in ids.chunked(self._RETRIEVE_LIMIT)
            )
        )
        if is_single:
            return result[0]
        return result
