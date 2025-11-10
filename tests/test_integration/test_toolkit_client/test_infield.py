from cognite.client.data_classes.data_modeling import Space

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.infield import InfieldLocationConfig
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestInfieldConfig:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient, toolkit_space: Space) -> None:
        config = InfieldLocationConfig.model_validate(
            {
                "space": toolkit_space.space,
                "externalId": f"test_crud_infield_config_{RUN_UNIQUE_ID}",
                "rootLocationExternalId": "test_crud_infield_config",
                "dataExplorationConfig": {"observations": {"enabled": True}},
            }
        )

        try:
            created_list = toolkit_client.infield.config.apply([config])
            assert len(created_list) == 2, (
                "Expected 2 configs to be created (data exploration config and infield location config)"
            )
            created = created_list[0]
            assert created.as_id() == config.as_id()

            retrieved_configs = toolkit_client.infield.config.retrieve([config.as_id()])
            assert len(retrieved_configs) == 1
            assert retrieved_configs[0].dump() == config.dump()

            deleted = toolkit_client.infield.config.delete([config])
            assert len(deleted) == 2, (
                "Expected 2 configs to be deleted (data exploration config and infield location config)"
            )
            retrieved_configs = toolkit_client.infield.config.retrieve([config.as_id()])
            assert len(retrieved_configs) == 0
        finally:
            toolkit_client.data_modeling.instances.delete(
                [
                    (config.space, config.external_id),
                    (config.data_exploration_config.space, config.data_exploration_config.external_id),
                ]
            )
