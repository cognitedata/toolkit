from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml, CLIConfig, ModulesConfig
from cognite_toolkit._cdf_tk.plugins import Plugins
from tests.data import CDF_TOML_DATA


@pytest.fixture(autouse=True)
def non_singleton_cdf_toml() -> None:
    _ = CDFToml.load(CDF_TOML_DATA, use_singleton=True)


class TestPlugins:
    def test_list_all(self) -> None:
        plugins = Plugins.list()
        assert "run" in plugins
        assert "data" in plugins
        assert "dump" in plugins
        assert "dev" in plugins

    def test_plugin_is_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        cdf_toml_singleton = self._create_cdf_toml({"run": True, "data": True, "dump": False, "dev": False})
        monkeypatch.setattr(f"{Plugins.__module__}.CDFToml", cdf_toml_singleton)

        assert not Plugins.dump.value.is_enabled()
        assert not Plugins.dev.value.is_enabled()
        assert Plugins.run.value.is_enabled()
        assert Plugins.data.value.is_enabled()

    @staticmethod
    def _create_cdf_toml(plugins: dict[str, bool]) -> MagicMock:
        cdf_toml_singleton = MagicMock(spec=CDFToml)
        cdf_toml_singleton.load_module_context.return_value = CDFToml(
            cdf=CLIConfig(Path()), modules=ModulesConfig(version="0.0.0"), plugins=plugins
        )
        return cdf_toml_singleton
