from collections.abc import Iterable

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    RESOURCE_BASE_IO_BY_FOLDER_NAME_INCLUDE_ALPHA,
    RESOURCE_IO_BY_FOLDER_NAME,
)
from cognite_toolkit._cdf_tk.resource_ios._sap_writeback import SAPEndpointIO, SAPInstanceIO, SchemaMappingIO
from cognite_toolkit._cdf_tk.yaml_classes import SAPEndpointYAML, SAPInstanceYAML, SchemaMappingYAML
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources

_YAML_CLS_BY_KIND = {
    "SAPInstance": SAPInstanceYAML,
    "SAPEndpoint": SAPEndpointYAML,
    "SchemaMapping": SchemaMappingYAML,
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
        included = {loader.kind for loader in RESOURCE_BASE_IO_BY_FOLDER_NAME_INCLUDE_ALPHA["SAPwritebacks"]}
        assert included == {"SAPInstance", "SAPEndpoint", "SchemaMapping"}
        assert all(
            loader.folder_name == "SAPwritebacks"
            for loader in RESOURCE_BASE_IO_BY_FOLDER_NAME_INCLUDE_ALPHA["SAPwritebacks"]
        )
        if Flags.SAP_WRITEBACK.is_enabled():
            enabled = {loader.kind for loader in RESOURCE_IO_BY_FOLDER_NAME["SAPwritebacks"]}
            assert enabled == included
        else:
            assert "SAPwritebacks" not in RESOURCE_IO_BY_FOLDER_NAME

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
