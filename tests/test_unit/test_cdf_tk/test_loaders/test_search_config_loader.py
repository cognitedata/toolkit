from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.client.data_classes.search_config import (
    SearchConfig,
    SearchConfigView,
    SearchConfigWrite,
)
from cognite_toolkit._cdf_tk.loaders._resource_loaders.configuration_loader import SearchConfigLoader
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.data import LOAD_DATA


@pytest.fixture
def basic_search_config(env_vars_with_client: EnvironmentVariables) -> SearchConfigWrite:
    """Fixture to load a basic search configuration."""
    loader = SearchConfigLoader.create_loader(env_vars_with_client.get_client())
    raw_list = loader.load_resource_file(
        LOAD_DATA / "search_configs" / "basic.SearchConfig.yaml", env_vars_with_client.dump()
    )
    loaded = loader.load_resource(raw_list[0], is_dry_run=False)
    return loaded


@pytest.fixture
def complex_search_config(env_vars_with_client: EnvironmentVariables) -> SearchConfigWrite:
    """Fixture to load a complex search configuration."""
    loader = SearchConfigLoader.create_loader(env_vars_with_client.get_client())
    raw_list = loader.load_resource_file(
        LOAD_DATA / "search_configs" / "complex.SearchConfig.yaml", env_vars_with_client.dump()
    )
    loaded = loader.load_resource(raw_list[0], is_dry_run=False)
    return loaded


class TestSearchConfigLoader:
    def test_load_basic_search_config(self, env_vars_with_client: EnvironmentVariables) -> None:
        """Test loading a basic search configuration."""
        loader = SearchConfigLoader.create_loader(env_vars_with_client.get_client())
        raw_list = loader.load_resource_file(
            LOAD_DATA / "search_configs" / "basic.SearchConfig.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        assert isinstance(loaded, SearchConfigWrite)
        assert loaded.id == 1001
        assert loaded.view.external_id == "test-view"
        assert loaded.view.space == "test-space"
        assert loaded.use_as_name == "name"
        assert loaded.use_as_description == "description"
        assert len(loaded.column_layout) == 2
        assert loaded.column_layout[0].property == "name"
        assert loaded.column_layout[0].selected is True

    def test_load_complex_search_config(self, complex_search_config: SearchConfigWrite) -> None:
        """Test loading a complex search configuration with all layout types."""
        assert isinstance(complex_search_config, SearchConfigWrite)
        assert complex_search_config.id == 1002
        assert complex_search_config.view.external_id == "complex-view"
        assert complex_search_config.view.space == "complex-space"

    def test_create(self, env_vars_with_client: EnvironmentVariables) -> None:
        """Test create method by mocking the API call."""
        loader = SearchConfigLoader.create_loader(env_vars_with_client.get_client())
        view = SearchConfigView(external_id="test-view", space="test-space")
        config = SearchConfigWrite(view=view, id=1001)

        with patch.object(loader.client.search.configurations, "upsert") as mock_upsert:
            mock_upsert.return_value = SearchConfig(
                view=config.view,
                id=config.id,
                created_time=1625097600000,
                updated_time=1625097600000,
                use_as_name=config.use_as_name,
                use_as_description=config.use_as_description,
                column_layout=config.column_layout,
                filter_layout=config.filter_layout,
                properties_layout=config.properties_layout,
            )

            result = loader.create(config)
            mock_upsert.assert_called_once_with(config)
            assert len(result) == 1
            assert result[0].id == 1001
            assert result[0].view.external_id == "test-view"

    def test_retrieve(self, env_vars_with_client: EnvironmentVariables) -> None:
        """Test retrieve method by mocking the API call."""
        loader = SearchConfigLoader.create_loader(env_vars_with_client.get_client())

        with patch.object(loader.client.search.configurations, "list") as mock_list:
            mock_list.return_value = [
                SearchConfig(
                    view=SearchConfigView(external_id="view1", space="space1"),
                    id=1001,
                    created_time=1625097600000,
                    updated_time=1625097600000,
                ),
                SearchConfig(
                    view=SearchConfigView(external_id="view2", space="space2"),
                    id=1002,
                    created_time=1625097600000,
                    updated_time=1625097600000,
                ),
            ]

            result = loader.retrieve(["1001"])
            mock_list.assert_called_once()
            assert len(result) == 1
            assert result[0].id == 1001

    def test_update(self, env_vars_with_client: EnvironmentVariables) -> None:
        """Test update method which uses upsert under the hood."""
        loader = SearchConfigLoader.create_loader(env_vars_with_client.get_client())
        view = SearchConfigView(external_id="test-view", space="test-space")
        config = SearchConfigWrite(view=view, id=1001, use_as_name="updated_name")

        with patch.object(loader.client.search.configurations, "upsert") as mock_upsert:
            mock_upsert.return_value = SearchConfig(
                view=config.view,
                id=config.id,
                created_time=1625097600000,
                updated_time=1625097700000,
                use_as_name=config.use_as_name,
                use_as_description=config.use_as_description,
                column_layout=config.column_layout,
                filter_layout=config.filter_layout,
                properties_layout=config.properties_layout,
            )

            result = loader.update(config)
            mock_upsert.assert_called_once_with(config)
            assert len(result) == 1
            assert result[0].id == 1001
            assert result[0].use_as_name == "updated_name"
            assert result[0].updated_time == 1625097700000

        # Test update with no ID
        config_no_id = SearchConfigWrite(view=view)
        with pytest.raises(KeyError, match="Search Configuaration Update Requires Id"):
            loader.update(config_no_id)

    def test_iterate(self, env_vars_with_client: EnvironmentVariables) -> None:
        """Test the _iterate method."""
        loader = SearchConfigLoader.create_loader(env_vars_with_client.get_client())
        with patch.object(loader.client.search.configurations, "list") as mock_list:
            mock_list.return_value = [
                SearchConfig(
                    view=SearchConfigView(external_id="view1", space="space1"),
                    id=1001,
                    created_time=1625097600000,
                    updated_time=1625097600000,
                ),
                SearchConfig(
                    view=SearchConfigView(external_id="view2", space="space2"),
                    id=1002,
                    created_time=1625097600000,
                    updated_time=1625097600000,
                ),
            ]
            result = list(loader._iterate())
            mock_list.assert_called_once()
            assert len(result) == 2
            assert result[0].id == 1001
            assert result[1].id == 1002
