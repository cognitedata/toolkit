from __future__ import annotations

import pytest

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.plugins import Plugin, Plugins
from tests.data import CDF_TOML_DATA


@pytest.fixture(autouse=True)
def non_singleton_cdf_toml() -> None:
    _ = CDFToml.load(CDF_TOML_DATA, use_singleton=True)


@pytest.mark.skip("This test is not working because we need to mock the CDFToml.load method first")
class TestPlugins:
    def test_list_all(self) -> None:
        plugins = Plugins.list()
        assert "run" in plugins
        assert "pull" in plugins
        assert "dump" in plugins

    def test_plugin_disabled(self) -> None:
        assert not Plugin.is_enabled(Plugins.dump)

    def test_plugin_enabled(self) -> None:
        assert Plugin.is_enabled(Plugins.run)
