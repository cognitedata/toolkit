from random import randint

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.search_config import (
    SearchConfig,
    SearchConfigList,
    SearchConfigView,
    SearchConfigWrite,
)

SEARCH_CONFIG_NAME = "Search Config Name"


@pytest.fixture(scope="session")
def existing_search_config(toolkit_client: ToolkitClient) -> dict:
    result: dict = {
        "id": None,
        "config": None,
        "total_configs": 0,
    }
    view = SearchConfigView(external_id="CogniteTimeSeries", space="cdf_cdm")
    search_config = SearchConfigWrite(view=view, use_as_name=SEARCH_CONFIG_NAME)

    configs = toolkit_client.search_configurations.list()
    if configs:
        result["total_configs"] = len(configs)
        for config in configs:
            if config.use_as_name == SEARCH_CONFIG_NAME:
                result["id"] = config.id
                result["config"] = config
                break

    if result["id"] is None:
        created = toolkit_client.search_configurations.update(search_config)
        result["id"] = created.id
        result["config"] = created
        result["total_configs"] += 1

    return result


class TestSearchConfigAPI:
    def test_list_search_configurations(self, toolkit_client: ToolkitClient, existing_search_config: dict) -> None:
        """Test that we can list search configurations."""
        search_configs = toolkit_client.search_configurations.list()
        assert isinstance(search_configs, SearchConfigList)
        assert len(search_configs) == existing_search_config["total_configs"]

    def test_update_search_configuration(self, toolkit_client: ToolkitClient, existing_search_config: dict) -> None:
        view = existing_search_config["config"].view
        test_description = f"Test description Update {randint(1000, 9999)}"
        test_nanme = SEARCH_CONFIG_NAME
        search_config = SearchConfigWrite(
            id=existing_search_config["id"],
            view=view,
            use_as_description=test_description,
            use_as_name=test_nanme,
        )

        updated = toolkit_client.search_configurations.update(search_config)
        assert isinstance(updated, SearchConfig)
        assert updated.id == existing_search_config["id"]
        assert updated.view.external_id == view.external_id
        assert updated.view.space == view.space
        assert updated.use_as_name == SEARCH_CONFIG_NAME
        assert updated.use_as_description == test_description

        configs = toolkit_client.search_configurations.list()
        found_config = next((c for c in configs if c.id == updated.id), None)
        assert found_config is not None
        assert found_config.use_as_name == SEARCH_CONFIG_NAME
