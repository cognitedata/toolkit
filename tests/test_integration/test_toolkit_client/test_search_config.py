import pytest
from cognite.client.data_classes.data_modeling import DataModel, DataModelApply, Space, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.search_config import (
    SearchConfig,
    SearchConfigList,
    SearchConfigView,
    SearchConfigWrite,
)
from tests.test_integration.constants import RUN_UNIQUE_ID

SEARCH_CONFIG_NAME = f"CONF_useAsName_{RUN_UNIQUE_ID}"


@pytest.fixture(scope="session")
def existing_search_config(toolkit_client: ToolkitClient) -> SearchConfig:
    view = SearchConfigView(external_id="CogniteTimeSeries", space="cdf_cdm")
    search_config = SearchConfigWrite(view=view, use_as_name=SEARCH_CONFIG_NAME)

    configs = toolkit_client.search_configurations.list()
    for config in configs:
        if config.use_as_name == SEARCH_CONFIG_NAME:
            return config

    created = toolkit_client.search_configurations.update(search_config)
    return created


@pytest.fixture(scope="session")
def my_data_model(toolkit_client: ToolkitClient, toolkit_space: Space) -> DataModel:
    data_model = toolkit_client.data_modeling.data_models.apply(
        DataModelApply(
            space=toolkit_space.space,
            external_id="SearchConfigTest",
            version="v1",
            views=[ViewId("cdf_cdm", "CogniteTimeSeries", "v1")],
        )
    )
    return data_model


class TestSearchConfigAPI:
    def test_update_and_list(self, toolkit_client: ToolkitClient) -> None:
        view = SearchConfigView(external_id="CogniteTimeSeries", space="cdf_cdm")
        test_name = f"{SEARCH_CONFIG_NAME}_test_update"
        search_config = SearchConfigWrite(view=view, use_as_name=test_name, use_as_description="Test description")

        created = toolkit_client.search_configurations.update(search_config)
        assert isinstance(created, SearchConfig)
        assert created.use_as_name == test_name
        assert created.use_as_description == "Test description"

        configs = toolkit_client.search_configurations.list()
        found_config = next((c for c in configs if c.id == created.id), None)
        assert found_config is not None
        assert found_config.use_as_name == test_name

    def test_list_search_configs(self, toolkit_client: ToolkitClient, existing_search_config: SearchConfig) -> None:
        search_configs = toolkit_client.search_configurations.list()
        assert isinstance(search_configs, SearchConfigList)
        assert len(search_configs) > 0

        # Check if our test config is in the list
        found = False
        for config in search_configs:
            if config.id == existing_search_config.id:
                found = True
                break
        assert found, "Existing search config not found in list"

    def test_update_existing_config(self, toolkit_client: ToolkitClient, existing_search_config: SearchConfig) -> None:
        """Test that we can update an existing search config."""
        updated_config = existing_search_config.as_write()
        updated_config.use_as_description = f"Updated description {RUN_UNIQUE_ID}"

        result = toolkit_client.search_configurations.update(updated_config)
        assert result.use_as_description == updated_config.use_as_description

        configs = toolkit_client.search_configurations.list()
        found_config = next((c for c in configs if c.id == existing_search_config.id), None)
        assert found_config is not None
        assert found_config.use_as_description == updated_config.use_as_description
