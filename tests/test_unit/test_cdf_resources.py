import toml

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import RESOURCES_PATH
from cognite_toolkit._cdf_tk.plugins import Plugins


class TestCDFToml:
    def test_plugins_are_up_to_date(self) -> None:
        """Test that the plugins in cdf.toml are up to date with the latest versions."""
        placeholder_file = RESOURCES_PATH / CDFToml.file_name
        placeholder_toml = toml.load(placeholder_file)

        assert "plugins" in placeholder_toml, "Missing 'plugins' section in cdf.toml"
        placeholder_plugins = placeholder_toml["plugins"]

        assert sorted(Plugins.list()) == sorted(placeholder_plugins), (
            "Plugins in cdf.toml are not up to date with the latest versions."
        )
