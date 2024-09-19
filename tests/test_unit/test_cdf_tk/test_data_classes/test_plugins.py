from __future__ import annotations

import pytest

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.plugins import Plugin, Plugins
from tests.data import CDF_TOML_DATA


@pytest.fixture(autouse=True)
def non_singleton_cdf_toml() -> None:
    _ = CDFToml.load(CDF_TOML_DATA, use_singleton=True)


class TestPlugins:
    def test_list_all(self) -> None:
        assert len(Plugins.list()) == 2

    def test_plugin_disabled(self) -> None:
        assert not Plugin.is_enabled(Plugins.dump)

    def test_plugin_enabled(self) -> None:
        assert Plugin.is_enabled(Plugins.graphql)
