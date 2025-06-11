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
            # Invalid: package type is unknown
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [package.bad_lib]
                url = "ftp://bad.com"
                """,
                "Invalid package configuration for 'bad_lib': URL must start with 'https'",
            ),
            # Invalid: package missing url for https type
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [package.missing_url]
                type = "https"
                """,
                "Invalid package configuration for 'missing_url': Library configuration must contain 'url' field.",
            ),
            # Invalid: package url is not a valid URL
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [package.invalid_url]
                type = "https"
                url = "bad.com"
                """,
                "Invalid package configuration for 'invalid_url': URL is missing scheme or network location (e.g., 'https://domain.com')",
            ),
            # Invalid: package url does not end with .zip
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [package.invalid_zip]
                type = "https"
                url = "https://example.com/my-package/my-package.txt"
                """,
                "Invalid package configuration for 'invalid_zip': URL must point to a .zip file.",
            ),
        ],
    )
    def test_load_invalid_toml_content(self, tmp_path: Path, invalid_toml_content: str, expected_error_message: str):
        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(invalid_toml_content)

        with pytest.raises(ToolkitTOMLFormatError, match=re.escape(expected_error_message)):
            CDFToml.load(cwd=tmp_path, use_singleton=False)

    def test_load_package_url(self, tmp_path: Path):
        valid_toml_content = """
        [cdf]
        [modules]
        version = "0.0.0"
        [package.valid_url]
        url = "https://github.com/cognitedata/package/archive/refs/tags/0.0.1.zip"
        """
        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(valid_toml_content)

        config = CDFToml.load(cwd=tmp_path, use_singleton=False)
        assert config.packages["valid_url"].url == "https://github.com/cognitedata/package/archive/refs/tags/0.0.1.zip"

    def test_fallback_to_official(self, tmp_path: Path):
        valid_toml_content = """
        [cdf]
        [modules]
        version = "0.0.0"
        """
        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(valid_toml_content)

        config = CDFToml.load(cwd=tmp_path, use_singleton=False)
        assert config.packages == {}
