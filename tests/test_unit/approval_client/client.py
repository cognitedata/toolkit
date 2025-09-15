from __future__ import annotations

import hashlib
import itertools
import json as JSON
from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any, BinaryIO, TextIO, cast
from unittest.mock import MagicMock

import pandas as pd
from cognite.client import CogniteClient
from cognite.client import data_modeling as dm
from cognite.client._api.iam import IAMAPI
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    Database,
    DataSet,
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineConfigWrite,
    FileMetadata,
    FunctionCall,
    Group,
    GroupList,
    Table,
    TableList,
    ThreeDModel,
    WorkflowVersion,
    capabilities,
)
from cognite.client.data_classes._base import CogniteResource, T_CogniteResource
from cognite.client.data_classes.capabilities import AllProjectsScope, ProjectCapability, ProjectCapabilityList
from cognite.client.data_classes.data_modeling import (
    DataModelList,
    Edge,
    EdgeApply,
    EdgeApplyResult,
    EdgeApplyResultList,
    EdgeId,
    EdgeList,
    InstancesApplyResult,
    InstancesDeleteResult,
    InstancesResult,
    Node,
    NodeApply,
    NodeApplyResult,
    NodeApplyResultList,
    NodeId,
    Space,
    VersionedDataModelingId,
    View,
)
from cognite.client.data_classes.data_modeling.graphql import DMLApplyResult
from cognite.client.data_classes.data_modeling.ids import DataModelIdentifier, InstanceId
from cognite.client.data_classes.functions import FunctionsStatus
from cognite.client.data_classes.iam import CreatedSession, GroupWrite, ProjectSpec, TokenInspection
from cognite.client.utils._text import to_camel_case
from cognite.client.utils.useful_types import SequenceNotStr
from requests import Response

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.graphql_data_models import GraphQLDataModelWrite
from cognite_toolkit._cdf_tk.client.data_classes.raw import RawDatabase
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock
from cognite_toolkit._cdf_tk.constants import INDEX_PATTERN
from cognite_toolkit._cdf_tk.cruds import FileCRUD
from cognite_toolkit._cdf_tk.utils import calculate_hash
from cognite_toolkit._cdf_tk.utils.auth import CLIENT_NAME

from .config import API_RESOURCES
from .data_classes import APIResource, AuthGroupCalls

TEST_FOLDER = Path(__file__).resolve().parent.parent

_ALL_CAPABILITIES = []
for cap, (scopes, names) in capabilities._VALID_SCOPES_BY_CAPABILITY.items():
    for action, scope in itertools.product(cap.Action, scopes):
        try:
            _ALL_CAPABILITIES.append(cap([action], scope=scope()))
        except TypeError:
            pass
del cap, scopes, names, action, scope


class LookUpAPIMock:
    def __init__(self, allow_reverse_lookup: bool = False):
        self._reverse_cache: dict[int, str] = {}
        self._allow_reverse_lookup = allow_reverse_lookup

    @staticmethod
    def create_id(string: str, allow_empty: bool = False) -> int:
        if allow_empty and string == "":
            return 0
        # This simulates CDF setting the internal ID.
        # By using hashing, we will always get the same ID for the same string.
        # Thus, the ID will be consistent between runs and can be used in snapshots.
        hash_object = hashlib.sha256(string.encode())
        hex_dig = hash_object.hexdigest()
        hash_int = int(hex_dig[:16], 16)
        return hash_int

    def id(
        self, external_id: str | SequenceNotStr[str], is_dry_run: bool = False, allow_empty: bool = False
    ) -> int | list[int]:
        if isinstance(external_id, str):
            id_ = self.create_id(external_id, allow_empty)
            if id_ not in self._reverse_cache:
                self._reverse_cache[id_] = external_id
            return id_
        output: list[int] = []
        for ext_id in external_id:
            id_ = self.create_id(ext_id, allow_empty)
            if id_ not in self._reverse_cache:
                self._reverse_cache[id_] = ext_id
            output.append(id_)
        return output

    @staticmethod
    def create_external_id(id: int | Sequence[int]) -> str | list[str]:
        """Creates an external ID from an ID."""
        if isinstance(id, int):
            return f"external_{id}"
        return [f"external_{i}" for i in id]

    def external_id(
        self,
        id: int | Sequence[int],
    ) -> str | list[str]:
        try:
            return self._reverse_cache[id] if isinstance(id, int) else [self._reverse_cache[i] for i in id]
        except KeyError:
            if self._allow_reverse_lookup:
                return self.create_external_id(id) if isinstance(id, int) else [self.create_external_id(i) for i in id]
            else:
                raise RuntimeError(f"{type(self).__name__} does not support reverse lookup before lookup")


class ApprovalToolkitClient:
    """A mock CogniteClient that is used for testing the clean, deploy commands
    of the cognite-toolkit.

    Args:
        mock_client: The mock client to use.

    """

    def __init__(self, mock_client: ToolkitClientMock, allow_reverse_lookup: bool = False):
        self._return_verify_resources = False
        self.mock_client = mock_client
        credentials = MagicMock(spec=OAuthClientCredentials)
        credentials.client_id = "toolkit-client-id"
        credentials.client_secret = "toolkit-client-secret"
        credentials.token_url = "https://toolkit.auth.com/oauth/token"
        credentials.scopes = ["ttps://pytest-field.cognitedata.com/.default"]
        self.mock_client.config = ToolkitClientConfig(
            client_name=CLIENT_NAME,
            project="pytest-project",
            credentials=credentials,
            is_strict_validation=False,
        )
        # This is used to simulate the existing resources in CDF
        self._existing_resources: dict[str, list[CogniteResource]] = defaultdict(list)
        # This is used to log all delete operations
        self._deleted_resources: dict[str, list[str | int | dict[str, Any]]] = defaultdict(list)
        # This is used to log all create operations
        self.created_resources: dict[str, list[CogniteResource | dict[str, Any]]] = defaultdict(list)

        # This is used to log all operations
        self._delete_methods: dict[str, list[MagicMock]] = defaultdict(list)
        self._create_methods: dict[str, list[MagicMock]] = defaultdict(list)
        self._retrieve_methods: dict[str, list[MagicMock]] = defaultdict(list)
        self._inspect_methods: dict[str, list[MagicMock]] = defaultdict(list)
        self._post_methods: dict[str, list[MagicMock]] = defaultdict(list)

        # Set the side effect of the MagicMock to the real method
        self.mock_client.iam.compare_capabilities.side_effect = IAMAPI.compare_capabilities
        self.mock_client.iam.sessions.create.return_value = CreatedSession(
            id=1234, status="READY", nonce="123", type="CLIENT_CREDENTIALS", client_id="12345-12345-12345-12345"
        )
        # Set functions to be activated
        self.mock_client.functions.status.return_value = FunctionsStatus(status="activated")
        self.mock_client.functions._zip_and_upload_folder.return_value = -1
        # Activate authorization_header()
        self.mock_client.config.credentials.authorization_header.return_value = ("Bearer", "123")
        # Set project
        self.mock_client.config.project = "test_project"
        self.mock_client.config.base_url = "https://bluefield.cognitedata.com"
        # Setup mock for all lookup methods
        for method_name, lookup_api in self.mock_client.lookup.__dict__.items():
            if method_name.startswith("_") or method_name == "method_calls":
                continue
            mock_lookup = LookUpAPIMock(allow_reverse_lookup)
            lookup_api.id.side_effect = mock_lookup.id
            lookup_api.external_id.side_effect = mock_lookup.external_id
        self.mock_client.verify.authorization.return_value = []

        # Setup all mock methods
        for resource in API_RESOURCES:
            parts = resource.api_name.split(".")
            mock_api = mock_client
            for part in parts:
                if not hasattr(mock_api, part):
                    raise ValueError(f"Invalid api name {resource.api_name}, could not find {part}")
                # To avoid registering the side effect on the mock_client.post.post and use
                # just mock_client.post instead, we need to skip the "step into" post mock here.
                if part != "post":
                    mock_api = getattr(mock_api, part)
            for method_type, methods in resource.methods.items():
                method_factory: Callable = {
                    "create": self._create_create_method,
                    "delete": self._create_delete_method,
                    "retrieve": self._create_retrieve_method,
                    "inspect": self._create_inspect_method,
                    "post": self._create_post_method,
                    "upsert": self._create_retrieve_method,
                    "update": self._create_post_method,
                }[method_type]
                method_dict = {
                    "create": self._create_methods,
                    "delete": self._delete_methods,
                    "retrieve": self._retrieve_methods,
                    "inspect": self._inspect_methods,
                    "post": self._post_methods,
                    "upsert": self._retrieve_methods,
                    "update": self._post_methods,
                }[method_type]
                for mock_method in methods:
                    if not hasattr(mock_api, mock_method.api_class_method):
                        raise ValueError(
                            f"Invalid api method {mock_method.api_class_method} for resource {resource.api_name}"
                        )
                    method = getattr(mock_api, mock_method.api_class_method)
                    method.side_effect = method_factory(resource, mock_method.mock_class_method, mock_client)
                    method_dict[resource.resource_cls.__name__].append(method)

    @property
    def client(self) -> CogniteClient:
        """Returns a mock CogniteClient"""
        return cast(CogniteClient, self.mock_client)

    @property
    def return_verify_resources(self) -> bool:
        return self._return_verify_resources

    @return_verify_resources.setter
    def return_verify_resources(self, value: bool) -> None:
        """This is used to return the resource that are used for verication.

        Caveat: This only applies to Spaces, DataSets, and ExtractionPipeline.

        The use case is that these are used in verification of other resources.
        """
        self._return_verify_resources = value

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

    def clear_cdf_resources(self, resource_cls: type[CogniteResource]) -> None:
        """Clears the existing resources in CDF.

        Args:
            resource_cls: The type of resource to clear.

        """
        self._existing_resources[resource_cls.__name__].clear()

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
            if isinstance(ids, VersionedDataModelingId | InstanceId):
                deleted.append(ids.dump(camel_case=True))
            elif isinstance(ids, Sequence):
                deleted.extend([id.dump(camel_case=True) for id in ids])
            if deleted:
                deleted_resources[resource_cls.__name__].extend(deleted)
            return deleted

        def delete_instances(
            nodes: NodeId | Sequence[NodeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
            edges: EdgeId | Sequence[EdgeId] | tuple[str, str] | Sequence[tuple[str, str]] | None = None,
        ) -> InstancesDeleteResult:
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

            if nodes:
                return InstancesDeleteResult(nodes=deleted, edges=[])
            elif edges:
                return InstancesDeleteResult(nodes=[], edges=deleted)
            else:
                return InstancesDeleteResult(nodes=[], edges=[])

        def delete_space(spaces: str | Sequence[str]) -> list:
            deleted = []
            if isinstance(spaces, str):
                deleted.append(spaces)
            elif isinstance(spaces, Sequence):
                deleted.extend(spaces)
            if deleted:
                deleted_resources[resource_cls.__name__].extend(deleted)
            return deleted

        def delete_raw(db_name: str | Sequence[str], name: str | Sequence[str] | None = None) -> list:
            if name:
                deleted = [{"db_name": db_name, "name": name if isinstance(name, str) else sorted(name)}]
            else:
                deleted = [{"db_name": name} for name in (db_name if isinstance(db_name, Sequence) else [db_name])]
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
        created_resources = self.created_resources
        write_resource_cls = resource.write_cls
        write_list_cls = resource.write_list_cls
        resource_cls = resource.resource_cls
        resource_list_cls = resource.list_cls

        def _create(*args, **kwargs) -> Sequence:
            created = []
            is_single_resource: bool | None = None
            for value in itertools.chain(args, kwargs.values()):
                if isinstance(value, Iterable):
                    values = value
                else:
                    values = [value]
                for item in values:
                    if isinstance(item, write_resource_cls):
                        created.append(item)
                        if is_single_resource is None:
                            is_single_resource = True
                        else:
                            is_single_resource = False
                    elif isinstance(item, Sequence) and all(isinstance(v, write_resource_cls) for v in item):
                        created.extend(item)
                        is_single_resource = False
                    elif isinstance(item, str) and issubclass(write_resource_cls, RawDatabase):
                        created.append(Database(name=item))
            created_resources[resource_cls.__name__].extend(created)
            if resource_cls is View:
                return write_list_cls(created)
            if resource_list_cls is GroupList:
                # Groups needs special handling to convert the write to read
                # to account for Unknown ACLs.
                return resource_list_cls(_group_write_to_read(c) for c in created)

            read_list = resource_list_cls.load(
                [
                    {
                        # These are server set fields, so we need to set them manually
                        # Note that many of them are only used for certain resources. This is not a problem
                        # as any extra fields are ignored by the load method.
                        "isGlobal": False,  # Data Modeling
                        "lastUpdatedTime": 0,
                        "createdTime": 0,
                        "writable": True,  # Data Modeling
                        "ignoreNullFields": False,  # Transformations
                        "usedFor": "nodes",  # Views
                        "timeSeriesCount": 10,  # Datapoint subscription
                        "updatedTime": 0,  # Robotics
                        "id": 42,  # LocationFilters
                        "status": "connected",  # Hosted Extractor Job
                        "targetStatus": "paused",  # Hosted Extractor Job
                        **c.dump(camel_case=True),
                    }
                    for c in created
                ],
                cognite_client=client,
            )
            return read_list

        def create_multiple(*args, **kwargs) -> Any:
            return _create(*args, **kwargs)

        def create_single(*args, **kwargs) -> Sequence:
            return _create(*args, **kwargs)[0]

        def create_filemetadata(*args, **kwargs) -> tuple[FileMetadata, str]:
            return create_single(*args, **kwargs), "upload link"

        def create_raw_table(db_name: str, name: str | list[str]) -> Table | TableList:
            if isinstance(name, str):
                created = Table(name=name, created_time=1)
                created_resources[resource_cls.__name__].append(created)
                return created
            else:
                created_list = TableList([Table(name=n, created_time=1) for n in name])
                created_resources[resource_cls.__name__].extend(created_list)
                return created_list

        def _group_write_to_read(group: GroupWrite) -> Group:
            return Group(
                name=group.name,
                source_id=group.source_id,
                capabilities=group.capabilities,
                metadata=group.metadata,
                members=group.members,
            )

        def upsert(*args, **kwargs) -> Any:
            upserted = []
            for value in itertools.chain(args, kwargs.values()):
                if isinstance(value, write_resource_cls):
                    upserted.append(value)
                elif isinstance(value, Sequence) and all(isinstance(v, write_resource_cls) for v in value):
                    upserted.extend(value)
            created_resources[resource_cls.__name__].extend(upserted)

            read_resource_objects: list[dict[str, object]] = [
                {
                    "isGlobal": False,
                    "lastUpdatedTime": 0,
                    "createdTime": 0,
                    **c.dump(camel_case=True),
                }
                for c in upserted
            ]
            if resource_cls is WorkflowVersion:
                for item in read_resource_objects:
                    if "workflowDefinition" not in item:
                        item["workflowDefinition"] = {}
                    if not isinstance(item["workflowDefinition"], dict):
                        raise TypeError(
                            f"Expected 'workflowDefinition' to be a dict, got {type(item['workflowDefinition'])}"
                        )
                    item["workflowDefinition"]["hash"] = "123"

            return resource_list_cls.load(read_resource_objects, cognite_client=client)

        def _create_dataframe_info(dataframe: pd.DataFrame) -> dict[str, Any]:
            return {
                "shape": "x".join(map(str, dataframe.shape)),
                "nan_count": int(dataframe.isna().sum().sum()),
                "null_count": int(dataframe.isnull().sum().sum()),
                "empty_count": int(dataframe[dataframe == ""].count().sum()),
                # We round float to 4 decimals places to avoid issues with floating point precision on different systems
                # as this is stored a snapshot.
                "first_row": dataframe.iloc[0].apply(lambda x: round(x, 4) if isinstance(x, float) else x).to_dict(),
                "last_row": dataframe.iloc[-1].apply(lambda x: round(x, 4) if isinstance(x, float) else x).to_dict(),
                "index_name": dataframe.index.name if dataframe.index.name else "missing",
            }

        def insert_dataframe(*args, **kwargs) -> None:
            args = list(args)
            kwargs = dict(kwargs)
            dataframe_info: dict[str, Any] = {}
            for arg in list(args):
                if isinstance(arg, pd.DataFrame):
                    args.remove(arg)
                    dataframe_info = _create_dataframe_info(arg)
                    break
            for key in list(kwargs):
                if isinstance(kwargs[key], pd.DataFrame):
                    value = kwargs.pop(key)
                    dataframe_info = _create_dataframe_info(value)
                    break
            if not dataframe_info:
                raise ValueError("No dataframe found in arguments")
            name = "_".join([str(arg) for arg in itertools.chain(args, kwargs.values())])
            if not name:
                name = "missing"
            created_resources[resource_cls.__name__].append(
                {"name": name, "args": args, "kwargs": kwargs, "dataframe": dataframe_info}
            )

        def upload(*args, **kwargs) -> None:
            name = ""
            for k, v in kwargs.items():
                if isinstance(v, Path) or (isinstance(v, str) and Path(v).exists()):
                    # The index pattern is used to ensure unique names. This index
                    # is removed as we do not care whether the order of the files are uploaded
                    filepath = Path(v)
                    filepath = filepath.with_name(INDEX_PATTERN.sub("", filepath.name))

                    try:
                        kwargs[k] = "/".join(filepath.relative_to(TEST_FOLDER).parts)
                    except ValueError:
                        kwargs[k] = "/".join(filepath.parts)
                    name = filepath.name

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
            created_nodes = []
            if isinstance(nodes, NodeApply):
                created_nodes.append(nodes)
            elif isinstance(nodes, Sequence) and all(isinstance(v, NodeApply) for v in nodes):
                created_nodes.extend(nodes)

            created_edges = []
            if isinstance(edges, EdgeApply):
                created_edges.append(edges)
            elif isinstance(edges, Sequence) and all(isinstance(v, EdgeApply) for v in edges):
                created_edges.extend(edges)

            if created_nodes and created_edges:
                raise ValueError(
                    "Cannot create both nodes and edges at the same time. Toolikt should call one at a time"
                )
            created_resources[Node.__name__].extend(created_nodes)
            created_resources[Edge.__name__].extend(created_edges)

            node_list = []
            if isinstance(nodes, Sequence):
                node_list.extend(nodes)
            elif isinstance(nodes, NodeApply):
                node_list.append(nodes)

            edge_list = []
            if isinstance(edges, Sequence):
                edge_list.extend(edges)
            elif isinstance(edges, EdgeApply):
                edge_list.append(edges)

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
                        for node in node_list
                    ]
                ),
                edges=EdgeApplyResultList(
                    [
                        EdgeApplyResult(
                            space=edge.space,
                            external_id=edge.external_id,
                            version=edge.existing_version or 1,
                            was_modified=True,
                            last_updated_time=1,
                            created_time=1,
                        )
                        for edge in edge_list
                    ]
                ),
            )

        def create_nodes(
            nodes: NodeApply | Sequence[NodeApply] | None = None,
            **_,
        ) -> NodeApplyResult | NodeApplyResultList:
            """Create nodes in the mock client."""
            output = create_instances(nodes=nodes, edges=None)
            if isinstance(nodes, NodeApply):
                return output.nodes[0]
            return output.nodes

        def create_extraction_pipeline_config(config: ExtractionPipelineConfigWrite) -> ExtractionPipelineConfig:
            created_resources[resource_cls.__name__].append(config)
            return ExtractionPipelineConfig.load(config.dump(camel_case=True))

        def upload_bytes_files_api(content: str | bytes | TextIO | BinaryIO, **kwargs) -> FileMetadata:
            if not isinstance(content, bytes):
                raise NotImplementedError("Only bytes content is supported")

            created_resources[resource_cls.__name__].append(
                {
                    **kwargs,
                }
            )
            return FileMetadata.load({to_camel_case(k): v for k, v in kwargs.items()})

        def upload_file_content_path_files_api(
            path: str,
            external_id: str | None = None,
            instance_id: NodeId | None = None,
        ) -> FileMetadata:
            return _upload_file_content_files_api(
                calculate_hash(Path(path), shorten=True), external_id=external_id, instance_id=instance_id
            )

        def upload_file_content_bytes_files_api(
            content: str,
            external_id: str | None = None,
            instance_id: NodeId | None = None,
        ) -> FileMetadata:
            return _upload_file_content_files_api(
                calculate_hash(content, shorten=True), external_id=external_id, instance_id=instance_id
            )

        def _upload_file_content_files_api(
            filehash: str,
            external_id: str | None = None,
            instance_id: NodeId | None = None,
        ) -> FileMetadata:
            if sum([bool(external_id), bool(instance_id)]) != 1:
                raise ValueError("Exactly one of external_id or instance_id must be set")

            if external_id:
                entry = {"external_id": external_id}
            else:
                entry = instance_id.dump(include_instance_type=False)
            entry["filehash"] = filehash

            created_resources[FileCRUD.__name__].append(entry)

            return FileMetadata(external_id, instance_id)

        def create_3dmodel(
            name: str, data_set_id: int | None = None, metadata: dict[str, str] | None = None
        ) -> ThreeDModel:
            created = ThreeDModel(name=name, data_set_id=data_set_id, metadata=metadata, created_time=1)
            created_resources[resource_cls.__name__].append(created)
            return created

        def apply_dml(
            id: dm.DataModelId,
            dml: str,
            name: str | None = None,
            description: str | None = None,
            previous_version: str | None = None,
            preserve_dml: bool | None = None,
        ) -> DMLApplyResult:
            created = GraphQLDataModelWrite(
                space=id.space,
                external_id=id.external_id,
                version=id.version,
                dml=dml,
                name=name,
                description=description,
                previous_version=previous_version,
                preserve_dml=preserve_dml,
            )
            created_resources[resource_cls.__name__].append(created)
            return DMLApplyResult(
                space=id.space,
                external_id=id.external_id,
                version=id.version,
                description=description,
                name=name,
                last_updated_time="1",
                created_time="1",
            )

        available_create_methods = {
            fn.__name__: fn
            for fn in [
                create_multiple,
                create_single,
                create_filemetadata,
                insert_dataframe,
                upload,
                upsert,
                create_instances,
                create_extraction_pipeline_config,
                upload_bytes_files_api,
                upload_file_content_path_files_api,
                upload_file_content_bytes_files_api,
                create_3dmodel,
                apply_dml,
                create_raw_table,
                create_nodes,
            ]
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

        def _create_verification_resource(*args, **kwargs):
            # Note that the written version of the resource does not contain the serves set variables,
            # so we need to set these manually
            if resource_cls is Space:
                ids = list(*args)
                spaces = [
                    Space(
                        space=space,
                        is_global=False,
                        last_updated_time=1,
                        created_time=1,
                    )
                    for space in ids
                ]
                return read_list_cls(spaces, cognite_client=client)
            elif resource_cls is DataSet:
                if "external_ids" in kwargs:
                    external_ids = kwargs["external_ids"]
                elif "external_id" in kwargs:
                    external_ids = [kwargs["external_id"]]
                else:
                    raise RuntimeError("No external_ids or external_id in kwargs")
                datasets = [
                    DataSet(
                        external_id=external_id,
                        name=external_id,
                        id=42,
                        last_updated_time=1,
                        created_time=1,
                    )
                    for external_id in external_ids
                ]
                return read_list_cls(datasets, cognite_client=client)
            elif resource_cls is ExtractionPipeline:
                if "external_ids" in kwargs:
                    external_ids = kwargs["external_ids"]
                elif "external_id" in kwargs:
                    external_ids = [kwargs["external_id"]]
                else:
                    raise RuntimeError("No external_ids or external_id in kwargs")
                pipelines = [
                    ExtractionPipeline(
                        external_id=external_id,
                        name=external_id,
                        data_set_id=42,
                        id=722,
                        last_updated_time=1,
                        created_time=1,
                    )
                    for external_id in external_ids
                ]
                return read_list_cls(pipelines, cognite_client=client)

            raise NotImplementedError(f"Return values not implemented for {resource_cls}")

        def return_values(*args, **kwargs):
            if self._return_verify_resources and resource_cls in {Space, DataSet, ExtractionPipeline}:
                return _create_verification_resource(*args, **kwargs)

            return read_list_cls(existing_resources[resource_cls.__name__], cognite_client=client)

        def return_data_models(
            ids: DataModelIdentifier | Sequence[DataModelIdentifier], inline_views: bool = False
        ) -> DataModelList:
            if not existing_resources[resource_cls.__name__]:
                return DataModelList([])
            id_set = {ids} if isinstance(ids, str | tuple | dm.DataModelId) else set(ids)
            to_return = read_list_cls([], cognite_client=client)
            for resource in existing_resources[resource_cls.__name__]:
                id_ = resource.as_id()
                if id_ in id_set or id_.as_tuple() in id_set or id_.as_tuple()[:2] in id_set:
                    to_return.append(resource)
            return to_return

        def iterate_values(*argd, **kwargs):
            list_ = return_values(*argd, **kwargs)
            return (value for value in list_)

        def return_instances(*args, **kwargs) -> InstancesResult:
            read_list = return_values(*args, **kwargs)
            return InstancesResult(nodes=read_list, edges=EdgeList([]))

        def return_value(*args, **kwargs):
            if value := existing_resources[resource_cls.__name__]:
                return read_list_cls(value, cognite_client=client)[0]
            elif self.return_verify_resources and resource_cls in {Space, DataSet, ExtractionPipeline}:
                return _create_verification_resource(*args, **kwargs)[0]
            else:
                return None

        def files_retrieve(id: int | None = None, external_id: str | None = None) -> FileMetadata:
            if id is not None:
                return FileMetadata(id=id, uploaded=True)
            elif external_id is not None:
                return FileMetadata(external_id=external_id, uploaded=True)
            else:
                return return_value(external_id=external_id)

        def data_model_retrieve(ids, *args, **kwargs):
            id_set = set(ids) if isinstance(ids, Sequence) else {ids}
            to_return = read_list_cls([], cognite_client=client)
            for resource in existing_resources[resource_cls.__name__]:
                id = resource.as_id()
                if id in id_set or (id.as_tuple() in id_set and id.as_tuple()[:2] in id_set):
                    to_return.append(resource)
            return to_return

        available_retrieve_methods = {
            fn.__name__: fn
            for fn in [
                return_values,
                return_value,
                data_model_retrieve,
                return_instances,
                files_retrieve,
                iterate_values,
                return_data_models,
            ]
        }
        if mock_method not in available_retrieve_methods:
            raise ValueError(
                f"Invalid mock retrieve method {mock_method} for resource {resource_cls.__name__}. Supported {available_retrieve_methods.keys()}"
            )
        method = available_retrieve_methods[mock_method]
        return method

    def _create_inspect_method(self, resource: APIResource, mock_method: str, client: CogniteClient) -> Callable:
        existing_resources = self._existing_resources
        resource_cls = resource.resource_cls

        def return_value(*args, **kwargs):
            if value := existing_resources[resource_cls.__name__]:
                return value[0]

            return TokenInspection(
                subject="test",
                projects=[ProjectSpec(url_name="test_project", groups=[123, 456])],
                capabilities=ProjectCapabilityList(
                    [
                        ProjectCapability(capability=capability, project_scope=AllProjectsScope())
                        for capability in _ALL_CAPABILITIES
                    ],
                    cognite_client=client,
                ),
            )

        available_inspect_methods = {
            fn.__name__: fn
            for fn in [
                return_value,
            ]
        }
        if mock_method not in available_inspect_methods:
            raise ValueError(
                f"Invalid mock retrieve method {mock_method} for resource {resource_cls.__name__}. Supported {available_inspect_methods.keys()}"
            )
        method = available_inspect_methods[mock_method]
        return method

    def _create_post_method(self, resource: APIResource, mock_method: str, client: CogniteClient) -> Callable:
        def post_method(
            url: str, json: dict[str, Any], params: dict[str, Any] | None = None, headers: dict[str, Any] | None = None
        ) -> Response:
            sessionResponse = Response()
            if url.endswith("/sessions"):
                sessionResponse.status_code = 200
                sessionResponse._content = b'{"items":[{"id":5192234284402249,"nonce":"QhlCnImCBwBNc72N","status":"READY","type":"ONESHOT_TOKEN_EXCHANGE"}]}'
            elif url.endswith("/functions/schedules"):
                sessionResponse.status_code = 201
                sessionResponse._content = str.encode(JSON.dumps(json))
            elif url.split("/")[-3] == "functions" and url.split("/")[-2].isdigit() and url.endswith("call"):
                sessionResponse.status_code = 201
                sessionResponse._content = str.encode(
                    JSON.dumps(FunctionCall(id=1, status="RUNNING").dump(camel_case=True))
                )
            else:
                raise ValueError(
                    f"The url {url} is called with post method, but not mocked. Please add in _create_post_method in approval.client.py"
                )
            return sessionResponse

        existing_resources = self._existing_resources
        resource_cls = resource.resource_cls

        def return_value(*args, **kwargs):
            return existing_resources[resource_cls.__name__][0]

        available_post_methods = {
            fn.__name__: fn
            for fn in [
                return_value,
                post_method,
            ]
        }
        if mock_method not in available_post_methods:
            raise ValueError(
                f"Invalid mock retrieve method {mock_method} for resource {resource_cls.__name__}. Supported {available_post_methods.keys()}"
            )
        method = available_post_methods[mock_method]
        return method

    def dump(self, sort: bool = True) -> dict[str, Any]:
        """This returns a dictionary with all the resources that have been created and deleted.

        The sorting is useful in snapshot testing, as it makes for a consistent output. If you want to check the order
        that the resources were created, you can set sort=False.

        Args:
            sort: If True, the resources will be sorted by externalId, dbName, name, or name[0] if externalId is not available.


        Returns:
            A dict with the resources that have been created and deleted, {resource_name: [resource, ...]}
        """
        dumped = {}
        if sort:
            created_resources = sorted(self.created_resources)
        else:
            created_resources = list(self.created_resources)
        for key in created_resources:
            values = self.created_resources[key]
            if values:
                dumped_resource = (value.dump(camel_case=True) if hasattr(value, "dump") else value for value in values)
                if sort:

                    def sort_key(v: dict[str, Any]) -> str:
                        for identifier_name in [
                            "externalId",
                            "external_id",
                            "dbName",
                            "space",
                            "name",
                            "workflowExternalId",
                        ]:
                            if identifier_name in v:
                                return v[identifier_name]
                        if "dbName" in v and "name" in v and isinstance(v["name"], list):
                            return v["dbName"] + "/" + v["name"][0]
                        if "transformationExternalId" in v and "destination" in v:
                            return v["transformationExternalId"] + v["destination"]
                        if "view" in v and "space" in v["view"] and "externalId" in v["view"]:
                            return v["view"]["space"] + "/" + v["view"]["externalId"]
                        raise ValueError(f"Could not find identifier in {v}")

                    dumped[key] = sorted(dumped_resource, key=sort_key)
                else:
                    dumped[key] = list(dumped_resource)

        # Standardize file paths
        for filemedata in dumped.get("FileMetadata", []):
            if "kwargs" in filemedata and "path" in filemedata["kwargs"] and "/" in filemedata["kwargs"]["path"]:
                filemedata["kwargs"]["path"] = filemedata["kwargs"]["path"].split("/")[-1]

        if self._deleted_resources:
            dumped["deleted"] = {}
            if sort:
                deleted_resources = sorted(self._deleted_resources)
            else:
                deleted_resources = list(self._deleted_resources)

            for key in deleted_resources:
                values = self._deleted_resources[key]

                def sort_deleted(x):
                    if not isinstance(x, dict):
                        return x
                    if "externalId" in x:
                        return x["externalId"]
                    if "db_name" in x and "name" in x and isinstance(x["name"], list):
                        return x["db_name"] + "/" + x["name"][0]
                    if "db_name" in x:
                        return x["db_name"]
                    return "missing"

                if values:
                    dumped["deleted"][key] = (
                        sorted(
                            values,
                            key=sort_deleted,
                        )
                        if sort
                        else list(values)
                    )

        return dumped

    def created_resources_of_type(self, resource_type: type[T_CogniteResource]) -> list[T_CogniteResource]:
        """This returns all the resources that have been created of a specific type.

        Args:
            resource_type: The type of resource to return, for example, 'TimeSeries', 'DataSet', 'Transformation'

        Returns:
            A list of all the resources that have been created of a specific type.
        """
        return self.created_resources.get(resource_type.__name__, [])

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

    def inspect_calls(self) -> dict[str, int]:
        """This returns all the calls that have been made to the mock client to the inspect method."""
        return {
            key: call_count
            for key, methods in self._inspect_methods.items()
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
        for r in API_RESOURCES:
            if r.api_name.count(".") == 1:
                api_name, sub_api = r.api_name.split(".")
            elif r.api_name.count(".") == 0:
                api_name, sub_api = r.api_name, ""
            else:
                raise ValueError(f"Invalid api name {r.api_name}")
            mocked_apis[api_name] |= {sub_api} if sub_api else set()
        # These are mocked in the __init__ method
        for name, method in self.mock_client.lookup.__dict__.items():
            if not isinstance(method, MagicMock) or name.startswith("_") or name.startswith("assert_"):
                continue
            mocked_apis["lookup"].add(name)
        mocked_apis["verify"] = {"authorization"}

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
        # This is mocked in the __init__
        not_mocked.pop("iam.sessions.create", None)
        return dict(not_mocked)

    def auth_create_group_calls(self) -> Iterable[AuthGroupCalls]:
        groups = cast(GroupList, self.created_resources[Group.__name__])
        groups = sorted(groups, key=lambda x: x.name)
        for name, group in itertools.groupby(groups, key=lambda x: x.name):
            yield AuthGroupCalls(name=name, calls=list(group))
