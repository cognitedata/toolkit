import itertools
from typing import Any, Sequence
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client._api.data_modeling.containers import ContainersAPI
from cognite.client._api.data_modeling.data_models import DataModelsAPI
from cognite.client._api.data_modeling.spaces import SpacesAPI
from cognite.client._api.data_modeling.views import ViewsAPI
from cognite.client._api.iam import GroupsAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import TransformationsAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes import GroupList, TimeSeriesList, TransformationList
from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling import ContainerList, DataModelList, SpaceList, ViewList
from cognite.client.testing import monkeypatch_cognite_client


@pytest.fixture
def cognite_client_approval() -> CogniteClient:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    with monkeypatch_cognite_client() as client:
        state: dict[str, CogniteResourceList] = {}
        client.iam.groups = create_mock_api(GroupsAPI, GroupList, state)
        client.timeseries = create_mock_api(TimeSeriesAPI, TimeSeriesList, state)
        client.transformations = create_mock_api(TransformationsAPI, TransformationList, state)
        client.data_modeling.containers = create_mock_api(ContainersAPI, ContainerList, state)
        client.data_modeling.views = create_mock_api(ViewsAPI, ViewList, state)
        client.data_modeling.data_models = create_mock_api(DataModelsAPI, DataModelList, state)
        client.data_modeling.spaces = create_mock_api(SpacesAPI, SpaceList, state)

        def dump() -> dict[str, Any]:
            dumped = {}
            for key in sorted(state):
                values = state[key]
                if values:
                    dumped[key] = sorted(values.dump(camel_case=True), key=lambda x: x.get("externalId", x.get("name")))
            return dumped

        client.dump = dump

        try:
            yield client

        finally:
            state.clear()


def create_mock_api(
    api_client: type[APIClient], read_list_cls: type[CogniteResourceList], state: dict[str, CogniteResourceList]
) -> MagicMock:
    mock = MagicMock(spec=api_client)
    mock.list.return_value = read_list_cls([])
    resource_cls = read_list_cls._RESOURCE
    state[resource_cls.__name__] = read_list_cls([])

    def create(*args, **kwargs) -> Any:
        created = []
        for value in itertools.chain(args, kwargs.values()):
            if isinstance(value, resource_cls):
                created.append(value)
            elif isinstance(value, Sequence) and all(isinstance(v, resource_cls) for v in value):
                created.extend(value)
        state[resource_cls.__name__].extend(created)
        return read_list_cls(created)

    if hasattr(api_client, "create"):
        mock.create = create
    elif hasattr(api_client, "apply"):
        mock.apply = create

    if hasattr(api_client, "upsert"):
        mock.upsert = create

    return mock
