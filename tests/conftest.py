from __future__ import annotations

import itertools
from collections.abc import Sequence
from hashlib import sha256
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from cognite.client import CogniteClient
from cognite.client._api.data_modeling.containers import ContainersAPI
from cognite.client._api.data_modeling.data_models import DataModelsAPI
from cognite.client._api.data_modeling.spaces import SpacesAPI
from cognite.client._api.data_modeling.views import ViewsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.iam import GroupsAPI
from cognite.client._api.raw import RawDatabasesAPI, RawRowsAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import TransformationsAPI, TransformationSchedulesAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    Database,
    DatabaseList,
    DataSetList,
    GroupList,
    RowList,
    TimeSeriesList,
    TransformationList,
    TransformationScheduleList,
)
from cognite.client.data_classes._base import CogniteResourceList
from cognite.client.data_classes.data_modeling import (
    ContainerApplyList,
    ContainerList,
    DataModelApplyList,
    DataModelList,
    SpaceApplyList,
    SpaceList,
    ViewApplyList,
    ViewList,
)
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
        client.data_sets = create_mock_api(DataSetsAPI, DataSetList, state)
        client.timeseries = create_mock_api(TimeSeriesAPI, TimeSeriesList, state)
        client.raw.databases = create_mock_api(RawDatabasesAPI, DatabaseList, state)
        client.transformations = create_mock_api(TransformationsAPI, TransformationList, state)
        client.transformations.schedules = create_mock_api(
            TransformationSchedulesAPI, TransformationScheduleList, state
        )
        client.data_modeling.containers = create_mock_api(ContainersAPI, ContainerList, state, ContainerApplyList)
        client.data_modeling.views = create_mock_api(ViewsAPI, ViewList, state, ViewApplyList)
        client.data_modeling.data_models = create_mock_api(DataModelsAPI, DataModelList, state, DataModelApplyList)
        client.data_modeling.spaces = create_mock_api(SpacesAPI, SpaceList, state, SpaceApplyList)
        client.raw.rows = create_mock_api(RawRowsAPI, RowList, state)

        def dump() -> dict[str, Any]:
            dumped = {}
            for key in sorted(state):
                values = state[key]
                if values:
                    dumped[key] = sorted(
                        [value.dump(camel_case=True) if hasattr(value, "dump") else value for value in values],
                        key=lambda x: x.get("externalId", x.get("name")),
                    )
            return dumped

        client.dump = dump

        try:
            yield client

        finally:
            state.clear()


def create_mock_api(
    api_client: type[APIClient],
    read_list_cls: type[CogniteResourceList],
    state: dict[str, CogniteResourceList],
    write_list_cls: type[CogniteResourceList] | None = None,
) -> MagicMock:
    mock = MagicMock(spec=api_client)
    mock.list.return_value = read_list_cls([])
    if hasattr(api_client, "retrieve"):
        mock.retrieve.return_value = read_list_cls([])
    if hasattr(api_client, "retrieve_multiple"):
        mock.retrieve_multiple.return_value = read_list_cls([])

    resource_cls = read_list_cls._RESOURCE
    write_list_cls = write_list_cls or read_list_cls
    write_resource_cls = write_list_cls._RESOURCE

    state[resource_cls.__name__] = write_list_cls([])

    def create(*args, **kwargs) -> Any:
        created = []
        for value in itertools.chain(args, kwargs.values()):
            if isinstance(value, write_resource_cls):
                created.append(value)
            elif isinstance(value, Sequence) and all(isinstance(v, write_resource_cls) for v in value):
                created.extend(value)
            elif isinstance(value, str) and issubclass(write_resource_cls, Database):
                created.append(Database(name=value))
        state[resource_cls.__name__].extend(created)
        return write_list_cls(created)

    def insert_dataframe(*args, **kwargs) -> None:
        args = list(args)
        kwargs = dict(kwargs)
        dataframe_hash = ""
        dataframe_cols = []
        for arg in list(args):
            if isinstance(arg, pd.DataFrame):
                args.remove(arg)
                dataframe_hash = sha256(
                    pd.util.hash_pandas_object(arg, index=True).values, usedforsecurity=False
                ).hexdigest()
                dataframe_cols = list(arg.columns)
                break

        for key in list(kwargs):
            if isinstance(kwargs[key], pd.DataFrame):
                value = kwargs.pop(key)
                dataframe_hash = sha256(
                    pd.util.hash_pandas_object(value, index=True).values, usedforsecurity=False
                ).hexdigest()
                dataframe_cols = list(value.columns)
                break
        if not dataframe_hash:
            raise ValueError("No dataframe found in arguments")
        name = "_".join([str(arg) for arg in itertools.chain(args, kwargs.values())])
        if not name:
            name = "_".join(dataframe_cols)
        state[resource_cls.__name__].append(
            {
                "name": name,
                "args": args,
                "kwargs": kwargs,
                "dataframe": dataframe_hash,
                "columns": dataframe_cols,
            }
        )

    if hasattr(api_client, "create"):
        mock.create = create
    elif hasattr(api_client, "apply"):
        mock.apply = create

    if hasattr(api_client, "upsert"):
        mock.upsert = create

    if hasattr(api_client, "insert_dataframe"):
        mock.insert_dataframe = insert_dataframe

    return mock
