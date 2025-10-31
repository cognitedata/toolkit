import re
from pathlib import Path

import pytest

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml, _read_toml
from cognite_toolkit._cdf_tk.constants import RESOURCES_PATH
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
                [library.bad_lib]
                url = "ftp://bad.com"
                checksum = "1234567890abcdef"
                """,
                "Invalid library configuration for 'bad_lib': URL must start with 'https'",
            ),
            # Invalid: library missing url for https type
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [library.missing_url]
                checksum = "1234567890abcdef"
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
                url = "bad.com"
                checksum = "1234567890abcdef"
                """,
                "Invalid library configuration for 'invalid_url': URL is missing scheme or network location (e.g., 'https://domain.com')",
            ),
            # Invalid: library url does not end with .zip
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [library.invalid_zip]
                url = "https://example.com/my-package/my-package.txt"
                checksum = "1234567890abcdef"
                """,
                "Invalid library configuration for 'invalid_zip': URL must point to a .zip file.",
            ),
            # Invalid: library checksum is not a valid hex string
            (
                """
                [cdf]
                [modules]
                version = "0.0.0"
                [library.invalid_checksum]
                url = "https://example.com/my-package/my-package.zip"
                """,
                "Library configuration must contain 'checksum' field",
            ),
        ],
        ids=[
            "unknown_package_type",
            "missing_url",
            "invalid_url",
            "invalid_zip",
            "invalid_checksum",
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
        [library.valid_url]
        url = "https://github.com/cognitedata/package/archive/refs/tags/0.0.1.zip"
        checksum = "1234567890abcdef"
        """
        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(valid_toml_content)

        config = CDFToml.load(cwd=tmp_path, use_singleton=False)
        assert config.libraries["valid_url"].url == "https://github.com/cognitedata/package/archive/refs/tags/0.0.1.zip"

    def test_default_resources_cdf_toml_has_valid_library_config(self) -> None:
        """Test that the default cdf.toml in resources has valid library configuration."""
        default_toml_data = _read_toml(RESOURCES_PATH / CDFToml.file_name)

        # Verify the structure matches what the code expects
        assert "library" in default_toml_data
        assert "toolkit-data" in default_toml_data["library"]
        assert "url" in default_toml_data["library"]["toolkit-data"]
        assert "checksum" in default_toml_data["library"]["toolkit-data"]

        # Verify the URL is a valid HTTPS URL pointing to toolkit-data
        url = default_toml_data["library"]["toolkit-data"]["url"]
        assert url.startswith("https://github.com/cognitedata/toolkit-data")
        assert url.endswith(".zip")

        # Verify the checksum has the correct format
        checksum = default_toml_data["library"]["toolkit-data"]["checksum"]
        assert checksum.startswith("sha256:")
        assert len(checksum) == 73  # "sha256:" + 64 hex characters
