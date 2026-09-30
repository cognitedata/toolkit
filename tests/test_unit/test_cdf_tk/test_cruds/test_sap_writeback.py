from collections.abc import Iterable
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, WritebackRequestId
from cognite_toolkit._cdf_tk.client.resource_classes.sap_writeback import SAPInstanceRequest, SAPInstanceResponse
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import CRUDS_BY_FOLDER_NAME, CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA
from cognite_toolkit._cdf_tk.resource_ios._sap_writeback import (
    SAPEndpointIO,
    SAPInstanceIO,
    SchemaMappingIO,
    WritebackRequestIO,
)
from cognite_toolkit._cdf_tk.yaml_classes import (
    SAPEndpointYAML,
    SAPInstanceYAML,
    SchemaMappingYAML,
    WritebackRequestYAML,
)
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources

_YAML_CLS_BY_KIND = {
    "SAPInstance": SAPInstanceYAML,
    "SAPEndpoint": SAPEndpointYAML,
    "SchemaMapping": SchemaMappingYAML,
    "WritebackRequest": WritebackRequestYAML,
}


def _example_cases() -> Iterable:
    base = COMPLETE_ORG_ALPHA_FLAGS / MODULES
    for kind, yaml_cls in _YAML_CLS_BY_KIND.items():
        for case in find_resources(kind, resource_dir="SAPwritebacks", base=base):
            yield pytest.param(case.values[0], yaml_cls, id=str(case.id))


class TestSAPWritebackExamples:
    @pytest.mark.parametrize("data, yaml_cls", list(_example_cases()))
    def test_example_roundtrip(self, data: dict[str, object], yaml_cls: type) -> None:
        loaded = yaml_cls.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data


class TestSAPWritebackIO:
    def test_registered_behind_sap_writeback_flag(self) -> None:
        included = {loader.kind for loader in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA["SAPwritebacks"]}
        assert included == {"SAPInstance", "SAPEndpoint", "SchemaMapping", "WritebackRequest"}
        assert all(
            loader.folder_name == "SAPwritebacks" for loader in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA["SAPwritebacks"]
        )
        if Flags.SAP_WRITEBACK.is_enabled():
            enabled = {loader.kind for loader in CRUDS_BY_FOLDER_NAME["SAPwritebacks"]}
            assert enabled == included
        else:
            assert "SAPwritebacks" not in CRUDS_BY_FOLDER_NAME

    def test_endpoint_depends_on_instance_and_mapping(self) -> None:
        resource = SAPEndpointYAML.model_validate(
            {
                "externalId": "sap_endpoint_001",
                "endpointType": "notification",
                "instanceId": "sap_instance_001",
                "mappingId": "schema_mapping_001",
            }
        )
        assert set(SAPEndpointIO.get_dependencies(resource)) == {
            (SAPInstanceIO, ExternalId(external_id="sap_instance_001")),
            (SchemaMappingIO, ExternalId(external_id="schema_mapping_001")),
        }

    def test_instance_dump_keeps_local_password(self) -> None:
        io = SAPInstanceIO(MagicMock())
        response = SAPInstanceResponse(
            external_id="sap_instance_001",
            gateway_url="https://sap.example.com",
            client=100,
            username="sap-user",
            created_time=1,
            last_updated_time=2,
        )
        dumped = io.dump_resource(
            response,
            {"externalId": "sap_instance_001", "password": "secret"},
        )
        assert dumped == {
            "externalId": "sap_instance_001",
            "gatewayUrl": "https://sap.example.com",
            "client": 100,
            "username": "sap-user",
            "password": "secret",
        }
        request = SAPInstanceRequest(
            external_id="sap_instance_001",
            gateway_url="https://sap.example.com",
            client=100,
            username="sap-user",
            password="secret",
        )
        assert set(io.sensitive_strings(request)) == {"secret"}

    def test_writeback_request_id_is_not_sent_on_create(self) -> None:
        client = MagicMock()
        client.sap_writeback.create.return_value = []
        io = WritebackRequestIO(client)
        loaded = io.load_resource(
            {
                "requestId": "request-001",
                "endpointId": "sap_endpoint_001",
                "status": "pending",
                "request": [{"key": "observation-1", "payload": {"NotificationText": "Test"}}],
            }
        )

        assert io.get_id(loaded) == WritebackRequestId(request_id="request-001")
        io.create([loaded])
        sent = client.sap_writeback.create.call_args.args[0]
        assert [item.dump() for item in sent] == [
            {
                "endpointId": "sap_endpoint_001",
                "request": [{"key": "observation-1", "payload": {"NotificationText": "Test"}}],
            }
        ]

    def test_writeback_request_cannot_be_deleted(self) -> None:
        io = WritebackRequestIO(MagicMock())
        with pytest.raises(ToolkitNotSupported, match="cannot be deleted"):
            io.delete([WritebackRequestId(request_id="request-001")])
