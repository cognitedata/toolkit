import re
from pathlib import Path

import pytest

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.exceptions import ToolkitTOMLFormatError
from tests.constants import REPO_ROOT


# Reset singleton before each test to ensure test isolation
@pytest.fixture(autouse=True)
def reset_cdf_toml_singleton():
    global _CDF_TOML
    _CDF_TOML = None
    yield
    _CDF_TOML = None  # Clean up after test as well


class TestCDFToml:
    def test_load_repo_root_config(self) -> None:
        config = CDFToml.load(REPO_ROOT)

        assert config.modules.version == _version.__version__

    @pytest.mark.parametrize(
        "invalid_toml_content, expected_error_message",
        [
            # Invalid: library type is unknown
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [library.bad_lib]
                type = "ftp"
                url = "ftp://bad.com"
                """,
                "Invalid library configuration for 'bad_lib': Supported library type is 'https'",
            ),
            # Invalid: library missing url for https type
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [library.missing_url]
                type = "https"
                """,
                "Invalid library configuration for 'missing_url': Library configuration must contain 'url' field.",
            ),
            # Invalid: library url is not a valid URL
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [library.invalid_url]
                type = "https"
                url = "bad.com"
                """,
                "Invalid library configuration for 'invalid_url': For type 'https', 'url' field must be a valid URL.",
            ),
        ],
    )
    def test_load_invalid_toml_content(self, tmp_path: Path, invalid_toml_content: str, expected_error_message: str):
        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(invalid_toml_content)

        with pytest.raises(ToolkitTOMLFormatError, match=re.escape(expected_error_message)):
            CDFToml.load(cwd=tmp_path, use_singleton=False)

    def test_fallback_to_official(self):
        pass
