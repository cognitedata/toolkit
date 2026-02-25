import pytest
from cognite.client.data_classes.data_modeling import ContainerApply, Space, SpaceApply, ViewApply

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    InFieldCDMLocationConfigRequest,
    InFieldLocationConfigRequest,
)
from tests.data import INFIELD_CDM_LOCATION_CONFIG_CONTAINER_YAML, INFIELD_CDM_LOCATION_CONFIG_VIEW_YAML
from tests.test_integration.constants import RUN_UNIQUE_ID
from tests.test_integration.helpers import retry_on_deadlock


class TestInfieldConfig:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient, toolkit_space: Space) -> None:
        config = InFieldLocationConfigRequest.model_validate(
            {
                "space": toolkit_space.space,
                "externalId": f"test_crud_infield_config_{RUN_UNIQUE_ID}",
                "rootLocationExternalId": "test_crud_infield_config",
                "dataExplorationConfig": {"observations": {"enabled": True}},
            }
        )
        try:
            created_list = toolkit_client.infield.config.create([config])
            assert len(created_list) == 1
            created = created_list[0]
            assert created.external_id == config.external_id

            retrieved_configs = toolkit_client.infield.config.retrieve([config.as_id()])
            assert len(retrieved_configs) == 1
            retrieved = retrieved_configs[0]
            assert retrieved.data_exploration_config is not None
            assert retrieved.data_exploration_config.observations == {"enabled": True}

            deleted = retry_on_deadlock(lambda: toolkit_client.infield.config.delete([config.as_id()]))
            assert len(deleted) == 1
            retrieved_configs = toolkit_client.infield.config.retrieve([config.as_id()])
            assert len(retrieved_configs) == 0
        finally:
            toolkit_client.infield.config.delete([config.as_id()])


@pytest.fixture
def deploy_infield_cdm_location_config(toolkit_client: ToolkitClient) -> None:
    client = toolkit_client
    view = ViewApply.load(INFIELD_CDM_LOCATION_CONFIG_VIEW_YAML.read_text(encoding="utf-8"))
    if client.data_modeling.views.retrieve(view.as_id()):
        # View already exists
        return None
    space = SpaceApply(space=view.space)
    _ = client.data_modeling.spaces.apply([space])
    container = ContainerApply.load(INFIELD_CDM_LOCATION_CONFIG_CONTAINER_YAML.read_text(encoding="utf-8"))
    _ = client.data_modeling.containers.apply([container])
    _ = client.data_modeling.views.apply([view])
    return None


class TestInFieldCDMConfig:
    @pytest.mark.usefixtures("deploy_infield_cdm_location_config")
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient, toolkit_space: Space) -> None:
        config = InFieldCDMLocationConfigRequest.model_validate(
            {
                "space": toolkit_space.space,
                "externalId": f"test_crud_infield_cdm_config_{RUN_UNIQUE_ID}",
                "name": "Test CDM Location Config",
                "description": "Test configuration for InField CDM",
                "dataExplorationConfig": {"observations": {"enabled": True}},
            }
        )

        try:
            created_list = toolkit_client.infield.cdm_config.create([config])
            assert len(created_list) == 1, "Expected 1 config to be created (InField CDM location config)"
            created = created_list[0]
            assert created.as_id() == config.as_id()

            retrieved_configs = toolkit_client.infield.cdm_config.retrieve([config.as_id()])
            assert len(retrieved_configs) == 1
            assert retrieved_configs[0].as_request_resource().dump() == config.dump()

            deleted = retry_on_deadlock(lambda: toolkit_client.infield.cdm_config.delete([config.as_id()]))
            assert len(deleted) == 1, "Expected 1 config to be deleted (InField CDM location config)"
            retrieved_configs = toolkit_client.infield.cdm_config.retrieve([config.as_id()])
            assert len(retrieved_configs) == 0
        finally:
            toolkit_client.data_modeling.instances.delete([(config.space, config.external_id)])
