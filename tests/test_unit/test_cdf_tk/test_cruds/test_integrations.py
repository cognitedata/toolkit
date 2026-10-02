from collections.abc import Iterable

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    RESOURCE_BUILD_IO_BY_FOLDER_NAME,
    RESOURCE_BUILD_IO_BY_FOLDER_NAME_INCLUDE_ALPHA,
)
from cognite_toolkit._cdf_tk.resource_ios._integrations import IntegrationConfigsIO, IntegrationsIO
from cognite_toolkit._cdf_tk.yaml_classes import IntegrationConfigYAML, IntegrationYAML
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources

_YAML_CLS_BY_KIND = {
    "Integration": IntegrationYAML,
    "IntegrationConfig": IntegrationConfigYAML,
}


def _example_cases() -> Iterable:
    base = COMPLETE_ORG_ALPHA_FLAGS / MODULES
    for kind, yaml_cls in _YAML_CLS_BY_KIND.items():
        for case in find_resources(kind, resource_dir="integrations", base=base):
            yield pytest.param(case.values[0], yaml_cls, id=str(case.id))


class TestIntegrationExamples:
    @pytest.mark.parametrize("data, yaml_cls", list(_example_cases()))
    def test_example_roundtrip(self, data: dict[str, object], yaml_cls: type) -> None:
        loaded = yaml_cls.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data


class TestIntegrationsIO:
    def test_registered_behind_integrations_flag(self) -> None:
        included = {loader.kind for loader in RESOURCE_BUILD_IO_BY_FOLDER_NAME_INCLUDE_ALPHA["integrations"]}
        assert included == {"Integration", "IntegrationConfig"}
        assert all(
            loader.folder_name == "integrations"
            for loader in RESOURCE_BUILD_IO_BY_FOLDER_NAME_INCLUDE_ALPHA["integrations"]
        )
        if Flags.INTEGRATIONS.is_enabled():
            enabled = {loader.kind for loader in RESOURCE_BUILD_IO_BY_FOLDER_NAME["integrations"]}
            assert enabled == included
        else:
            assert "integrations" not in RESOURCE_BUILD_IO_BY_FOLDER_NAME

    def test_config_depends_on_integration(self) -> None:
        resource = IntegrationConfigYAML.model_validate(
            {"externalId": "pump.integration", "config": {"endpoint": "opc.tcp://plant.example"}}
        )
        assert set(IntegrationConfigsIO.get_dependencies(resource)) == {
            (IntegrationsIO, ExternalId(external_id="pump.integration")),
        }
