from __future__ import annotations

import hashlib
import itertools
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, cast
from unittest.mock import MagicMock

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.data_classes import (
    Database,
    DatabaseList,
    DatabaseWrite,
    DatabaseWriteList,
    Datapoints,
    DatapointsList,
    DataSet,
    DataSetList,
    DataSetWrite,
    DataSetWriteList,
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineConfigWrite,
    ExtractionPipelineConfigWriteList,
    ExtractionPipelineList,
    ExtractionPipelineWrite,
    ExtractionPipelineWriteList,
    FileMetadata,
    FileMetadataList,
    Group,
    GroupList,
    GroupWrite,
    GroupWriteList,
    Row,
    RowList,
    RowWrite,
    RowWriteList,
    Table,
    TableList,
    TableWrite,
    TableWriteList,
    TimeSeries,
    TimeSeriesList,
    TimeSeriesWrite,
    TimeSeriesWriteList,
    Transformation,
    TransformationList,
    TransformationSchedule,
    TransformationScheduleList,
    TransformationScheduleWrite,
    TransformationScheduleWriteList,
    TransformationWrite,
    TransformationWriteList,
)
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList, T_CogniteResource
from cognite.client.data_classes.data_modeling import (
    Container,
    ContainerApply,
    ContainerApplyList,
    ContainerList,
    DataModel,
    DataModelApply,
    DataModelApplyList,
    DataModelList,
    EdgeApply,
    EdgeApplyResultList,
    EdgeId,
    InstancesApplyResult,
    Node,
    NodeApply,
    NodeApplyList,
    NodeApplyResult,
    NodeApplyResultList,
    NodeId,
    NodeList,
    Space,
    SpaceApply,
    SpaceApplyList,
    SpaceList,
    VersionedDataModelingId,
    View,
    ViewApply,
    ViewApplyList,
    ViewList,
)
from cognite.client.data_classes.data_modeling.ids import InstanceId
from cognite.client.testing import CogniteClientMock

from cognite_toolkit.cdf_tk.load import ExtractionPipelineConfigList

TEST_FOLDER = Path(__file__).resolve().parent


class ApprovalCogniteClient:
    """A mock CogniteClient that is used for testing the clean, deploy commands
    of the cognite-toolkit.

    Args:
        mock_client: The mock client to use.

    """

    def __init__(self, mock_client: CogniteClientMock):
        self.mock_client = mock_client
        # This is used to simulate the existing resources in CDF
        self._existing_resources: dict[str, list[CogniteResource]] = defaultdict(list)
        # This is used to log all delete operations
        self._deleted_resources: dict[str, list[str | int | dict[str, Any]]] = defaultdict(list)
        # This is used to log all create operations
        self._created_resources: dict[str, list[CogniteResource | dict[str, Any]]] = defaultdict(list)

        # This is used to log all operations
        self._delete_methods: dict[str, list[MagicMock]] = defaultdict(list)
        self._create_methods: dict[str, list[MagicMock]] = defaultdict(list)
        self._retrieve_methods: dict[str, list[MagicMock]] = defaultdict(list)

        # Setup all mock methods
        for resource in _API_RESOURCES:
            parts = resource.api_name.split(".")
            mock_api = mock_client
            for part in parts:
                if not hasattr(mock_api, part):
                    raise ValueError(f"Invalid api name {resource.api_name}, could not find {part}")
                mock_api = getattr(mock_api, part)
            for method_type, methods in resource.methods.items():
                method_factory: Callable = {
                    "create": self._create_create_method,
                    "delete": self._create_delete_method,
                    "retrieve": self._create_retrieve_method,
                }[method_type]
                method_dict = {
                    "create": self._create_methods,
                    "delete": self._delete_methods,
                    "retrieve": self._retrieve_methods,
                }[method_type]
                for mock_method in methods:
                    if not hasattr(mock_api, mock_method.api_class_method):
                        raise ValueError(
                            f"Invalid api method {mock_method.api_class_method} for resource {resource.api_name}"
                        )
                    method = getattr(mock_api, mock_method.api_class_method)
                    method.side_effect = method_factory(resource, mock_method.mock_name, mock_client)
                    method_dict[resource.resource_cls.__name__].append(method)

    @property
    def client(self) -> CogniteClient:
        return cast(CogniteClient, self.mock_client)

    def append(self, resource_cls: type[CogniteResource], items: CogniteResource | Sequence[CogniteResource]) -> None:
        """This is used to simulate existing resources in CDF.

        Args:
            resource_cls: The type of resource this is.
            items: The list of resources to append.

        """
        if isinstance(items, Sequence):
            self._existing_resources[resource_cls.__name__].extend(items)
        else:
            self._existing_resources[resource_cls.__name__].append(items)

    def _create_delete_method(self, resource: APIResource, mock_method: str, client: CogniteClient) -> Callable:
        deleted_resources = self._deleted_resources
        resource_cls = resource.resource_cls

        def delete_id_external_id(
            id: int | Sequence[int] | None = None,
            external_id: str | Sequence[str] | None = None,
            **_,
        ) -> list:
            deleted = []
            if not isinstance(id, str) and isinstance(id, Sequence):
                deleted.extend({"id": i} for i in id)
            elif isinstance(id, int):
                deleted.append({"id": id})
            if isinstance(external_id, str):
                deleted.append({"externalId": external_id})
            elif isinstance(external_id, Sequence):
                deleted.extend({"externalId": i} for i in external_id)
            if deleted:
                deleted_resources[resource_cls.__name__].extend(deleted)
            return deleted

        def delete_data_modeling(ids: VersionedDataModelingId | Sequence[VersionedDataModelingId]) -> list:
            deleted = []
            if isinstance(ids, (VersionedDataModelingId, InstanceId)):
                deleted.append(ids.dump(camel_case=True))
            elif isinstance(ids, Sequence):
                deleted.extend([id.dump(camel_case=True) for id in ids])
            if deleted:
                deleted_resources[resource_cls.__name__].extend(deleted)
            return deleted

        def delete_instances(
            nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
            edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        ) -> list:
            deleted = []
            if isinstance(nodes, NodeId):
                deleted.append(nodes.dump(camel_case=True, include_instance_type=True))
            elif isinstance(nodes, tuple):
                deleted.append(NodeId(*nodes).dump(camel_case=True, include_instance_type=True))
            elif isinstance(edges, EdgeId):
                deleted.append(edges.dump(camel_case=True, include_instance_type=True))
            elif isinstance(edges, tuple):
                deleted.append(EdgeId(*edges).dump(camel_case=True, include_instance_type=True))
            elif isinstance(nodes, Sequence):
                deleted.extend(
                    [
                        node.dump(camel_case=True, include_instance_type=True) if isinstance(node, NodeId) else node
                        for node in nodes
                    ]
                )
            elif isinstance(edges, Sequence):
                deleted.extend(
                    [
                        edge.dump(camel_case=True, include_instance_type=True) if isinstance(edge, EdgeId) else edge
                        for edge in edges
                    ]
                )

            if deleted:
                deleted_resources[resource_cls.__name__].extend(deleted)
            return deleted

        def delete_space(spaces: str | Sequence[str]) -> list:
            deleted = []
            if isinstance(spaces, str):
                deleted.append(spaces)
            elif isinstance(spaces, Sequence):
                deleted.extend(spaces)
            if deleted:
                deleted_resources[resource_cls.__name__].extend(deleted)
            return deleted

        def delete_raw(db_name: str, name: str | Sequence[str]) -> list:
            deleted = [{"db_name": db_name, "name": name if isinstance(name, str) else sorted(name)}]
            deleted_resources[resource_cls.__name__].extend(deleted)
            return deleted

        available_delete_methods = {
            fn.__name__: fn
            for fn in [
                delete_id_external_id,
                delete_instances,
                delete_raw,
                delete_data_modeling,
                delete_space,
            ]
        }
        if mock_method not in available_delete_methods:
            raise ValueError(
                f"Invalid mock delete method {mock_method} for resource {resource_cls.__name__}. "
                f"Supported {list(available_delete_methods)}"
            )

        method = available_delete_methods[mock_method]
        return method

    def _create_create_method(self, resource: APIResource, mock_method: str, client: CogniteClient) -> Callable:
        created_resources = self._created_resources
        write_resource_cls = resource.write_cls
        write_list_cls = resource.write_list_cls
        resource_cls = resource.resource_cls
        resource_list_cls = resource.list_cls

        def create(*args, **kwargs) -> Any:
            created = []
            for value in itertools.chain(args, kwargs.values()):
                if isinstance(value, write_resource_cls):
                    created.append(value)
                elif isinstance(value, Sequence) and all(isinstance(v, write_resource_cls) for v in value):
                    created.extend(value)
                elif isinstance(value, str) and issubclass(write_resource_cls, Database):
                    created.append(Database(name=value))
            created_resources[resource_cls.__name__].extend(created)
            if resource_cls is View:
                return write_list_cls(created)
            if resource_cls is ExtractionPipelineConfig:
                print("stop")
            return resource_list_cls.load(
                [
                    {
                        "isGlobal": False,
                        "lastUpdatedTime": 0,
                        "createdTime": 0,
                        "writable": True,
                        "ignoreNullFields": False,
                        "usedFor": "nodes",
                        **c.dump(camel_case=True),
                    }
                    for c in created
                ],
                cognite_client=client,
            )

        def insert_dataframe(*args, **kwargs) -> None:
            args = list(args)
            kwargs = dict(kwargs)
            dataframe_hash = ""
            dataframe_cols = []
            for arg in list(args):
                if isinstance(arg, pd.DataFrame):
                    args.remove(arg)
                    dataframe_hash = int(pd.util.hash_pandas_object(arg, index=True, encoding="utf8").sum())
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
            created_resources[resource_cls.__name__].append(
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

            created_resources[resource_cls.__name__].append(
                {
                    "name": name,
                    "args": list(args),
                    "kwargs": dict(kwargs),
                }
            )

        def create_instances(
            nodes: NodeApply | Sequence[NodeApply] | None = None,
            edges: EdgeApply | Sequence[EdgeApply] | None = None,
            **kwargs,
        ) -> InstancesApplyResult:
            created = []
            if isinstance(nodes, NodeApply):
                created.append(nodes)
            elif isinstance(nodes, Sequence) and all(isinstance(v, NodeApply) for v in nodes):
                created.extend(nodes)
            if edges is not None:
                raise NotImplementedError("Edges not supported yet")
            created_resources[resource_cls.__name__].extend(created)
            return InstancesApplyResult(
                nodes=NodeApplyResultList(
                    [
                        NodeApplyResult(
                            space=node.space,
                            external_id=node.external_id,
                            version=node.existing_version or 1,
                            was_modified=True,
                            last_updated_time=1,
                            created_time=1,
                        )
                        for node in (nodes if isinstance(nodes, Sequence) else [nodes])
                    ]
                ),
                edges=EdgeApplyResultList([]),
            )

        def create_extraction_pipeline_config(config: ExtractionPipelineConfigWrite) -> ExtractionPipelineConfig:
            created_resources[resource_cls.__name__].append(config)
            return ExtractionPipelineConfig.load(config.dump(camel_case=True))

        available_create_methods = {
            fn.__name__: fn
            for fn in [create, insert_dataframe, upload, create_instances, create_extraction_pipeline_config]
        }
        if mock_method not in available_create_methods:
            raise ValueError(
                f"Invalid mock create method {mock_method} for resource {resource_cls.__name__}. Supported {list(available_create_methods.keys())}"
            )
        method = available_create_methods[mock_method]
        return method

    def _create_retrieve_method(self, resource: APIResource, mock_method: str, client: CogniteClient) -> Callable:
        existing_resources = self._existing_resources
        resource_cls = resource.resource_cls
        read_list_cls = resource.list_cls

        def return_values(*args, **kwargs):
            return read_list_cls(existing_resources[resource_cls.__name__], cognite_client=client)

        def return_value(*args, **kwargs):
            return read_list_cls(existing_resources[resource_cls.__name__], cognite_client=client)[0]

        available_retrieve_methods = {
            fn.__name__: fn
            for fn in [
                return_values,
                return_value,
            ]
        }
        if mock_method not in available_retrieve_methods:
            raise ValueError(
                f"Invalid mock retrieve method {mock_method} for resource {resource_cls.__name__}. Supported {available_retrieve_methods.keys()}"
            )
        method = available_retrieve_methods[mock_method]
        return method

    def dump(self) -> dict[str, Any]:
        """This returns a dictionary with all the resources that have been created and deleted.

        Returns:
            A dict with the resources that have been created and deleted, {resource_name: [resource, ...]}
        """
        dumped = {}
        for key in sorted(self._created_resources):
            values = self._created_resources[key]
            if values:
                dumped[key] = sorted(
                    [value.dump(camel_case=True) if hasattr(value, "dump") else value for value in values],
                    key=lambda x: x.get("externalId", x.get("dbName", x.get("db_name", x.get("name")))),
                )
        if self._deleted_resources:
            dumped["deleted"] = {}
            for key in sorted(self._deleted_resources):
                values = self._deleted_resources[key]

                def sort_deleted(x):
                    if not isinstance(x, dict):
                        return x
                    if "externalId" in x:
                        return x["externalId"]
                    if "db_name" in x and "name" in x and isinstance(x["name"], list):
                        return x["db_name"] + "/" + x["name"][0]
                    return "missing"

                if values:
                    dumped["deleted"][key] = sorted(
                        values,
                        key=sort_deleted,
                    )

        return dumped

    def created_resources_of_type(self, resource_type: type[T_CogniteResource]) -> list[T_CogniteResource]:
        """This returns all the resources that have been created of a specific type.

        Args:
            resource_type: The type of resource to return, for example, 'TimeSeries', 'DataSet', 'Transformation'

        Returns:
            A list of all the resources that have been created of a specific type.
        """
        return self._created_resources.get(resource_type.__name__, [])

    def create_calls(self) -> dict[str, int]:
        """This returns all the calls that have been made to the mock client to create methods.

        For example, if you have mocked the 'time_series' API, and the code you test calls the 'time_series.create' method,
        then this method will return {'time_series': 1}
        """
        return {
            key: call_count
            for key, methods in self._create_methods.items()
            if (call_count := sum(method.call_count for method in methods))
        }

    def retrieve_calls(self) -> dict[str, int]:
        """This returns all the calls that have been made to the mock client to retrieve methods.

        For example, if you have mocked the 'time_series' API, and the code you test calls the 'time_series.list' method,
        then this method will return {'time_series': 1}
        """
        return {
            key: call_count
            for key, methods in self._retrieve_methods.items()
            if (call_count := sum(method.call_count for method in methods))
        }

    def delete_calls(self) -> dict[str, int]:
        """This returns all the calls that have been made to the mock client to delete methods.

        For example, if you have mocked the 'time_series' API, and the code you test calls the 'time_series.delete' method,
        then this method will return {'time_series': 1}
        """
        return {
            key: call_count
            for key, methods in self._delete_methods.items()
            if (call_count := sum(method.call_count for method in methods))
        }

    def not_mocked_calls(self) -> dict[str, int]:
        """This returns all the calls that have been made to the mock client to sub APIs that have not been mocked.

        For example, if you have not mocked the 'time_series' API, and the code you test calls the 'time_series.list' method,
        then this method will return {'time_series.list': 1}

        Returns:
            A dict with the calls that have been made to sub APIs that have not been mocked, {api_name.method_name: call_count}
        """
        mocked_apis: dict[str : set[str]] = defaultdict(set)
        for r in _API_RESOURCES:
            if r.api_name.count(".") == 1:
                api_name, sub_api = r.api_name.split(".")
            elif r.api_name.count(".") == 0:
                api_name, sub_api = r.api_name, ""
            else:
                raise ValueError(f"Invalid api name {r.api_name}")
            mocked_apis[api_name] |= {sub_api} if sub_api else set()

        not_mocked: dict[str, int] = defaultdict(int)
        for api_name, api in vars(self.mock_client).items():
            if not isinstance(api, MagicMock) or api_name.startswith("_") or api_name.startswith("assert_"):
                continue
            mocked_sub_apis = mocked_apis.get(api_name, set())
            for method_name in dir(api):
                if method_name.startswith("_") or method_name.startswith("assert_"):
                    continue
                method = getattr(api, method_name)
                if api_name not in mocked_apis and isinstance(method, MagicMock) and method.call_count:
                    not_mocked[f"{api_name}.{method_name}"] += method.call_count
                if hasattr(method, "_spec_set") and method._spec_set and method_name not in mocked_sub_apis:
                    # this is a sub api that must be checked
                    for sub_method_name in dir(method):
                        if sub_method_name.startswith("_") or sub_method_name.startswith("assert_"):
                            continue
                        sub_method = getattr(method, sub_method_name)
                        if isinstance(sub_method, MagicMock) and sub_method.call_count:
                            not_mocked[f"{api_name}.{method_name}.{sub_method_name}"] += sub_method.call_count
        return dict(not_mocked)

    def auth_create_group_calls(self) -> Iterable[AuthGroupCalls]:
        groups = cast(GroupList, self._created_resources[Group.__name__])
        groups = sorted(groups, key=lambda x: x.name)
        for name, group in itertools.groupby(groups, key=lambda x: x.name):
            yield AuthGroupCalls(name=name, calls=list(group))


@dataclass
class AuthGroupCalls:
    name: str
    calls: list[Group]

    @property
    def last_created_capabilities(self) -> set[str]:
        return {c._capability_name for c in self.calls[-1].capabilities}

    @property
    def capabilities_all_calls(self) -> set[str]:
        return {c._capability_name for call in self.calls for c in call.capabilities}


@dataclass
class Method:
    """Represent a method in the CogniteClient that should be mocked

    Args:
        api_class_method: The name of the method in the CogniteClient, for example, 'create', 'insert_dataframe'
        mock_name: The name of the method in the ApprovalCogniteClient, for example, 'create', 'insert_dataframe'

    The available mock methods you can see inside
    * ApprovalCogniteClient._create_create_method,
    * ApprovalCogniteClient._create_delete_method,
    * ApprovalCogniteClient._create_retrieve_method

    """

    api_class_method: str
    mock_name: str


@dataclass
class APIResource:
    """This is used to define the resources that should be mocked in the ApprovalCogniteClient

    Args:
        api_name: The name of the resource in the CogniteClient, for example, 'time_series', 'data_modeling.views'
        resource_cls: The resource class for the API
        list_cls: The list resource API class
        methods: The methods that should be mocked
        _write_cls: The write resource class for the API. For example, the writing class for 'data_modeling.views' is 'ViewApply'
        _write_list_cls: The write list class in the CogniteClient

    """

    api_name: str
    resource_cls: type[CogniteResource]
    list_cls: type[CogniteResourceList] | type[list]
    methods: dict[Literal["create", "delete", "retrieve"], list[Method]]

    _write_cls: type[CogniteResource] | None = None
    _write_list_cls: type[CogniteResourceList] | None = None

    @property
    def write_cls(self) -> type[CogniteResource]:
        return self._write_cls or self.resource_cls

    @property
    def write_list_cls(self) -> type[CogniteResourceList]:
        return self._write_list_cls or self.list_cls


# This is used to define the resources that should be mocked in the ApprovalCogniteClient
# You can add more resources here if you need to mock more resources
_API_RESOURCES = [
    APIResource(
        api_name="iam.groups",
        resource_cls=Group,
        _write_cls=GroupWrite,
        _write_list_cls=GroupWriteList,
        list_cls=GroupList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_id_external_id")],
            "retrieve": [Method(api_class_method="list", mock_name="return_values")],
        },
    ),
    APIResource(
        api_name="data_sets",
        resource_cls=DataSet,
        _write_cls=DataSetWrite,
        _write_list_cls=DataSetWriteList,
        list_cls=DataSetList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_values"),
                Method(api_class_method="retrieve_multiple", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="time_series",
        resource_cls=TimeSeries,
        _write_cls=TimeSeriesWrite,
        list_cls=TimeSeriesList,
        _write_list_cls=TimeSeriesWriteList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_values"),
                Method(api_class_method="retrieve_multiple", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="raw.databases",
        resource_cls=Database,
        _write_cls=DatabaseWrite,
        list_cls=DatabaseList,
        _write_list_cls=DatabaseWriteList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create")],
            "retrieve": [Method(api_class_method="list", mock_name="return_values")],
            "delete": [Method(api_class_method="delete", mock_name="delete_raw")],
        },
    ),
    APIResource(
        api_name="raw.tables",
        resource_cls=Table,
        _write_cls=TableWrite,
        list_cls=TableList,
        _write_list_cls=TableWriteList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create")],
            "retrieve": [Method(api_class_method="list", mock_name="return_values")],
            "delete": [Method(api_class_method="delete", mock_name="delete_raw")],
        },
    ),
    APIResource(
        api_name="raw.rows",
        resource_cls=Row,
        _write_cls=RowWrite,
        list_cls=RowList,
        _write_list_cls=RowWriteList,
        methods={
            "create": [Method(api_class_method="insert_dataframe", mock_name="insert_dataframe")],
            "delete": [Method(api_class_method="delete", mock_name="delete_raw")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="transformations",
        resource_cls=Transformation,
        _write_cls=TransformationWrite,
        list_cls=TransformationList,
        _write_list_cls=TransformationWriteList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_value"),
                Method(api_class_method="retrieve_multiple", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="transformations.schedules",
        resource_cls=TransformationSchedule,
        _write_cls=TransformationScheduleWrite,
        list_cls=TransformationScheduleList,
        _write_list_cls=TransformationScheduleWriteList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_value"),
                Method(api_class_method="retrieve_multiple", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="extraction_pipelines",
        resource_cls=ExtractionPipeline,
        _write_cls=ExtractionPipelineWrite,
        list_cls=ExtractionPipelineList,
        _write_list_cls=ExtractionPipelineWriteList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_value"),
                Method(api_class_method="retrieve_multiple", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="extraction_pipelines.config",
        resource_cls=ExtractionPipelineConfig,
        _write_cls=ExtractionPipelineConfigWrite,
        list_cls=ExtractionPipelineConfigList,
        _write_list_cls=ExtractionPipelineConfigWriteList,
        methods={
            "create": [Method(api_class_method="create", mock_name="create_extraction_pipeline_config")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_value"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.containers",
        resource_cls=Container,
        list_cls=ContainerList,
        _write_cls=ContainerApply,
        _write_list_cls=ContainerApplyList,
        methods={
            "create": [Method(api_class_method="apply", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_data_modeling")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.views",
        resource_cls=View,
        list_cls=ViewList,
        _write_cls=ViewApply,
        _write_list_cls=ViewApplyList,
        methods={
            "create": [Method(api_class_method="apply", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_data_modeling")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.data_models",
        resource_cls=DataModel,
        list_cls=DataModelList,
        _write_cls=DataModelApply,
        _write_list_cls=DataModelApplyList,
        methods={
            "create": [Method(api_class_method="apply", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_data_modeling")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.spaces",
        resource_cls=Space,
        list_cls=SpaceList,
        _write_cls=SpaceApply,
        _write_list_cls=SpaceApplyList,
        methods={
            "create": [Method(api_class_method="apply", mock_name="create")],
            "delete": [Method(api_class_method="delete", mock_name="delete_space")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="time_series.data",
        resource_cls=Datapoints,
        list_cls=DatapointsList,
        methods={
            "create": [
                Method(api_class_method="insert", mock_name="create"),
                Method(api_class_method="insert_dataframe", mock_name="insert_dataframe"),
            ],
        },
    ),
    APIResource(
        api_name="files",
        resource_cls=FileMetadata,
        list_cls=FileMetadataList,
        methods={
            "create": [Method(api_class_method="upload", mock_name="upload")],
            "delete": [Method(api_class_method="delete", mock_name="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_value"),
                Method(api_class_method="retrieve_multiple", mock_name="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.instances",
        resource_cls=Node,
        list_cls=NodeList,
        _write_cls=NodeApply,
        _write_list_cls=NodeApplyList,
        methods={
            "create": [Method(api_class_method="apply", mock_name="create_instances")],
            "delete": [Method(api_class_method="delete", mock_name="delete_instances")],
            "retrieve": [
                Method(api_class_method="list", mock_name="return_values"),
                Method(api_class_method="retrieve", mock_name="return_values"),
            ],
        },
    ),
]
