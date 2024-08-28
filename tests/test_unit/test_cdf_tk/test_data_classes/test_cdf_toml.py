from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from tests.constants import REPO_ROOT


class TestCDFToml:
    def test_load_repo_root_config(self) -> None:
        config = CDFToml.load(REPO_ROOT)

        assert config.modules.version == _version.__version__
