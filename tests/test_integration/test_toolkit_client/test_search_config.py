from random import randint

import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.search_config import (
    SearchConfig,
    SearchConfigWrite,
    ViewId,
)

SEARCH_CONFIG_NAME = "Search Config Name"


@pytest.fixture(scope="session")
def existing_search_config(toolkit_client: ToolkitClient) -> SearchConfig:
    view = ViewId(external_id="CogniteTimeSeries", space="cdf_cdm")
    search_config = SearchConfigWrite(view=view, use_as_name=SEARCH_CONFIG_NAME)

    configs = toolkit_client.search.configurations.list()
    if configs:
        for config in configs:
            if config.view == view:
                return config

    created = toolkit_client.search.configurations.upsert(search_config)
    return created


class TestSearchConfigAPI:
    def test_update_search_configuration(
        self, toolkit_client: ToolkitClient, existing_search_config: SearchConfig
    ) -> None:
        view = existing_search_config.view
        test_description = f"Test description Update {randint(1000, 9999)}"
        test_name = SEARCH_CONFIG_NAME
        search_config = SearchConfigWrite(
            id=existing_search_config.id,
            view=view,
            use_as_description=test_description,
            use_as_name=test_name,
        )

        updated = toolkit_client.search.configurations.upsert(search_config)
        assert isinstance(updated, SearchConfig)
        assert updated.id == existing_search_config.id
        assert updated.view.external_id == view.external_id
        assert updated.view.space == view.space
        assert updated.use_as_name == SEARCH_CONFIG_NAME
        assert updated.use_as_description == test_description

        configs = toolkit_client.search.configurations.list()
        found_config = next((c for c in configs if c.id == updated.id), None)
        assert found_config is not None
        assert found_config.use_as_name == SEARCH_CONFIG_NAME
