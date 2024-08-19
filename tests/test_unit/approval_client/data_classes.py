from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from cognite.client.data_classes import Group
from cognite.client.data_classes._base import CogniteResource, CogniteResourceList
from cognite.client.data_classes.capabilities import Capability, UnknownAcl


@dataclass
class AuthGroupCalls:
    name: str
    calls: list[Group]

    @property
    def last_created_capabilities(self) -> set[str]:
        return {self._get_capability_name(c) for c in self.calls[-1].capabilities}

    @property
    def capabilities_all_calls(self) -> set[str]:
        return {self._get_capability_name(c) for call in self.calls for c in call.capabilities}

    def _get_capability_name(self, capability: Capability) -> str:
        return capability.capability_name if isinstance(capability, UnknownAcl) else capability._capability_name


@dataclass
class Method:
    """Represent a method in the CogniteClient that should be mocked

    Args:
        api_class_method: The name of the method in the CogniteClient, for example, 'create', 'insert_dataframe'
        mock_class_method: The name of the method in the ApprovalCogniteClient, for example, 'create', 'insert_dataframe'

    The available mock methods you can see inside
    * ApprovalCogniteClient._create_create_method,
    * ApprovalCogniteClient._create_delete_method,
    * ApprovalCogniteClient._create_retrieve_method

    """

    api_class_method: str
    mock_class_method: str


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
