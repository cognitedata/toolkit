from __future__ import annotations

import hashlib
import itertools
from collections import defaultdict
from collections.abc import MutableSequence, Sequence
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pandas as pd
import pytest
from cognite.client import CogniteClient
from cognite.client._api.data_modeling.containers import ContainersAPI
from cognite.client._api.data_modeling.data_models import DataModelsAPI
from cognite.client._api.data_modeling.graphql import DataModelingGraphQLAPI
from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client._api.data_modeling.spaces import SpacesAPI
from cognite.client._api.data_modeling.views import ViewsAPI
from cognite.client._api.data_sets import DataSetsAPI
from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.files import FilesAPI
from cognite.client._api.iam import GroupsAPI
from cognite.client._api.raw import RawDatabasesAPI, RawRowsAPI
from cognite.client._api.time_series import TimeSeriesAPI
from cognite.client._api.transformations import TransformationsAPI, TransformationSchedulesAPI
from cognite.client._api_client import APIClient
from cognite.client.data_classes import (
    Database,
    DatabaseList,
    DatapointsList,
    DataSetList,
    FileMetadataList,
    GroupList,
    RowList,
    TimeSeriesList,
    TransformationList,
    TransformationScheduleList,
)
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes.data_modeling import (
    ContainerApplyList,
    ContainerList,
    DataModelApplyList,
    DataModelList,
    NodeApplyList,
    NodeList,
    SpaceApplyList,
    SpaceList,
    VersionedDataModelingId,
    ViewApplyList,
    ViewList,
)
from cognite.client.testing import monkeypatch_cognite_client

TEST_FOLDER = Path(__file__).resolve().parent


@pytest.fixture
def cognite_client_approval() -> CogniteClient:
    """
    Change directory to new_dir and return to the original directory when exiting the context.

    Args:
        new_dir: The new directory to change to.

    """
    with monkeypatch_cognite_client() as client:
        written_resources: dict[str, Sequence[CogniteResource | dict[str, Any]]] = {}
        deleted_resources: dict[str, list[str | int | dict[str, Any]]] = defaultdict(list)
        client.iam.groups = create_mock_api(GroupsAPI, GroupList, written_resources, deleted_resources)
        client.data_sets = create_mock_api(DataSetsAPI, DataSetList, written_resources, deleted_resources)
        client.time_series = create_mock_api(TimeSeriesAPI, TimeSeriesList, written_resources, deleted_resources)
        client.raw.databases = create_mock_api(RawDatabasesAPI, DatabaseList, written_resources, deleted_resources)
        client.transformations = create_mock_api(
            TransformationsAPI, TransformationList, written_resources, deleted_resources
        )
        client.transformations.schedules = create_mock_api(
            TransformationSchedulesAPI, TransformationScheduleList, written_resources, deleted_resources
        )
        client.data_modeling.containers = create_mock_api(
            ContainersAPI, ContainerList, written_resources, deleted_resources, ContainerApplyList
        )
        client.data_modeling.views = create_mock_api(
            ViewsAPI, ViewList, written_resources, deleted_resources, ViewApplyList
        )
        client.data_modeling.data_models = create_mock_api(
            DataModelsAPI, DataModelList, written_resources, deleted_resources, DataModelApplyList
        )
        client.data_modeling.spaces = create_mock_api(
            SpacesAPI, SpaceList, written_resources, deleted_resources, SpaceApplyList
        )
        client.raw.rows = create_mock_api(RawRowsAPI, RowList, written_resources, deleted_resources)
        client.time_series.data = create_mock_api(DatapointsAPI, DatapointsList, written_resources, deleted_resources)
        client.files = create_mock_api(FilesAPI, FileMetadataList, written_resources, deleted_resources)
        client.data_modeling.graphql = create_mock_api(
            DataModelingGraphQLAPI, DataModelList, written_resources, deleted_resources, DataModelApplyList
        )
        client.data_modeling.instances = create_mock_api(
            InstancesAPI, NodeList, written_resources, deleted_resources, NodeApplyList
        )

        def dump() -> dict[str, Any]:
            dumped = {}
            for key in sorted(written_resources):
                values = written_resources[key]
                if values:
                    dumped[key] = sorted(
                        [value.dump(camel_case=True) if hasattr(value, "dump") else value for value in values],
                        key=lambda x: x.get("externalId", x.get("name")),
                    )
            if deleted_resources:
                dumped["deleted"] = {}
                for key in sorted(deleted_resources):
                    dumped["deleted"][key] = sorted(
                        deleted_resources[key], key=lambda x: x.get("externalId") if isinstance(x, dict) else x
                    )

            return dumped

        client.dump = dump

        try:
            yield client

        finally:
            written_resources.clear()


def create_mock_api(
    api_client: type[APIClient],
    read_list_cls: type[CogniteResourceList],
    written_resources: dict[str, MutableSequence[CogniteResource | dict[str, Any]]],
    deleted_resources: dict[str, list[str | int | dict[str, Any]]],
    write_list_cls: type[CogniteResourceList] | None = None,
) -> MagicMock:
    mock = MagicMock(spec=api_client)
    if hasattr(api_client, "list"):
        mock.list.return_value = read_list_cls([])
    if hasattr(api_client, "retrieve"):
        mock.retrieve.return_value = read_list_cls([])
    if hasattr(api_client, "retrieve_multiple"):
        mock.retrieve_multiple.return_value = read_list_cls([])

    resource_cls = read_list_cls._RESOURCE
    write_list_cls = write_list_cls or read_list_cls
    write_resource_cls = write_list_cls._RESOURCE

    written_resources[resource_cls.__name__] = write_list_cls([])

    def create(*args, **kwargs) -> Any:
        created = []
        for value in itertools.chain(args, kwargs.values()):
            if isinstance(value, write_resource_cls):
                created.append(value)
            elif isinstance(value, Sequence) and all(isinstance(v, write_resource_cls) for v in value):
                created.extend(value)
            elif isinstance(value, str) and issubclass(write_resource_cls, Database):
                created.append(Database(name=value))
        written_resources[resource_cls.__name__].extend(created)
        return write_list_cls(created)

    def insert_dataframe(*args, **kwargs) -> None:
        args = list(args)
        kwargs = dict(kwargs)
        dataframe_hash = ""
        dataframe_cols = []
        for arg in list(args):
            if isinstance(arg, pd.DataFrame):
                args.remove(arg)
                dataframe_hash = int(hashlib.sha256(pd.util.hash_pandas_object(arg, index=True).values).hexdigest(), 16)
                dataframe_cols = list(arg.columns)
                break

        for key in list(kwargs):
            if isinstance(kwargs[key], pd.DataFrame):
                value = kwargs.pop(key)
                dataframe_hash = int(
                    hashlib.sha256(pd.util.hash_pandas_object(value, index=True).values).hexdigest(), 16
                )
                dataframe_cols = list(value.columns)
                break
        if not dataframe_hash:
            raise ValueError("No dataframe found in arguments")
        name = "_".join([str(arg) for arg in itertools.chain(args, kwargs.values())])
        if not name:
            name = "_".join(dataframe_cols)
        written_resources[resource_cls.__name__].append(
            {
                "name": name,
                "args": args,
                "kwargs": kwargs,
                "dataframe": dataframe_hash,
                "columns": dataframe_cols,
            }
        )

    def upload(*args, **kwargs) -> None:
        name = ""
        for k, v in kwargs.items():
            if isinstance(v, Path) or (isinstance(v, str) and Path(v).exists()):
                kwargs[k] = "/".join(Path(v).relative_to(TEST_FOLDER).parts)
                name = Path(v).name

        written_resources[resource_cls.__name__].append(
            {
                "name": name,
                "args": list(args),
                "kwargs": dict(kwargs),
            }
        )

    def apply_dml(*args, **kwargs):
        data = dict(kwargs)
        data["args"] = list(args)
        written_resources[resource_cls.__name__].append(data)

    def delete(*args, **kwargs) -> Any:
        deleted = []
        for arg in itertools.chain(args, kwargs.values()):
            # bool is a subclass of int in Python. Bool args are not arguments.
            if not isinstance(arg, bool) and isinstance(arg, (int, str, VersionedDataModelingId)):
                deleted.append(
                    arg.dump(camel_case=True, include_type=True) if isinstance(arg, VersionedDataModelingId) else arg
                )
            elif isinstance(arg, Sequence) and all(
                isinstance(v, (int, str, VersionedDataModelingId)) and not isinstance(v, bool) for v in arg
            ):
                deleted.extend(
                    [
                        v.dump(camel_case=True, include_type=True) if isinstance(v, VersionedDataModelingId) else v
                        for v in arg
                    ]
                )
        if deleted:
            deleted_resources[resource_cls.__name__].extend(deleted)
        return deleted

    if hasattr(api_client, "create"):
        mock.create = create
    elif hasattr(api_client, "apply"):
        mock.apply = create

    if hasattr(api_client, "upsert"):
        mock.upsert = create

    if hasattr(api_client, "insert_dataframe"):
        mock.insert_dataframe = insert_dataframe

    if hasattr(api_client, "upload"):
        mock.upload = upload

    if hasattr(api_client, "apply_dml"):
        mock.apply_dml = apply_dml

    if hasattr(api_client, "delete"):
        mock.delete = delete

    return mock
