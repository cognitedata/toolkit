import re
from pathlib import Path

import pytest

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk import cdf_toml as cdf_toml_module
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml, ModulesConfig, _set_plugin_enabled
from cognite_toolkit._cdf_tk.constants import RESOURCES_PATH
from cognite_toolkit._cdf_tk.data_classes._base import _load_version_variable
from cognite_toolkit._cdf_tk.exceptions import ToolkitTOMLFormatError, ToolkitVersionError
from tests.constants import REPO_ROOT


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
                """,
                "Invalid library configuration for 'invalid_zip': URL must point to a .zip file.",
            ),
        ],
        ids=[
            "unknown_package_type",
            "missing_url",
            "invalid_url",
            "invalid_zip",
        ],
    )
    def test_load_invalid_toml_content(
        self, tmp_path: Path, invalid_toml_content: str, expected_error_message: str
    ) -> None:
        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(invalid_toml_content)

        with pytest.raises(ToolkitTOMLFormatError, match=re.escape(expected_error_message)):
            CDFToml.load(cwd=tmp_path, use_singleton=False)

    def test_load_package_url(self, tmp_path: Path) -> None:
        valid_toml_content = """
        [cdf]
        [modules]
        version = "0.0.0"
        [library.valid_url]
        url = "https://github.com/cognitedata/package/archive/refs/tags/0.0.1.zip"
        """
        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(valid_toml_content)

        config = CDFToml.load(cwd=tmp_path, use_singleton=False)
        assert config.libraries["valid_url"].url == "https://github.com/cognitedata/package/archive/refs/tags/0.0.1.zip"

    def test_default_resources_cdf_toml_has_valid_library_config(self) -> None:
        """Test that the default cdf.toml in resources has valid library configuration."""
        default_cdf_toml = CDFToml.load(cwd=RESOURCES_PATH, use_singleton=False)

        # Verify the cognite library exists
        assert "cognite" in default_cdf_toml.libraries
        library = default_cdf_toml.libraries["cognite"]

        # Verify the URL is a valid HTTPS URL pointing to cognite library
        assert library.url.startswith("https://github.com/cognitedata/library")
        assert library.url.endswith(".zip")

    def test_modules_config_allows_any_module_version_when_cli_is_dev_version(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_version, "__version__", "0.0.0")
        monkeypatch.setattr("sys.argv", ["cdf", "build", "--env", "dev"])

        config = ModulesConfig.load({"version": "0.8.105"})

        assert config.version == "0.8.105"

    def test_modules_config_still_rejects_mismatch_when_cli_is_released(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_version, "__version__", "0.8.106")
        monkeypatch.setattr("sys.argv", ["cdf", "build", "--env", "dev"])

        with pytest.raises(ToolkitVersionError):
            ModulesConfig.load({"version": "0.8.105"})

    def test_system_version_allows_any_module_version_when_cli_is_dev_version(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(_version, "__version__", "0.0.0")

        version = _load_version_variable({"cdf_toolkit_version": "0.8.105"}, "_system.yaml")

        assert version == "0.8.105"

    def test_system_version_still_rejects_mismatch_when_cli_is_released(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(_version, "__version__", "0.8.106")

        with pytest.raises(ToolkitVersionError):
            _load_version_variable({"cdf_toolkit_version": "0.8.105"}, "_system.yaml")


class TestSetPluginEnabled:
    def test_replaces_existing_false_assignment(self) -> None:
        content = "[plugins]\ndump = false\ndev = false\n"

        result = _set_plugin_enabled(content, "dump")

        assert result == "[plugins]\ndump = true\ndev = false\n"

    def test_preserves_comments_and_other_sections(self) -> None:
        content = '[cdf]\ndefault_env = "dev"\n\n[plugins]\n# Toggle plugins here\ndump = false\n'

        result = _set_plugin_enabled(content, "dump")

        assert "# Toggle plugins here" in result
        assert 'default_env = "dev"' in result
        assert "dump = true" in result
        assert "dump = false" not in result

    def test_inserts_key_when_section_exists_without_key(self) -> None:
        content = "[plugins]\ndev = false\n"

        result = _set_plugin_enabled(content, "dump")

        assert result == "[plugins]\ndump = true\ndev = false\n"

    def test_appends_section_when_missing(self) -> None:
        content = '[cdf]\ndefault_env = "dev"\n'

        result = _set_plugin_enabled(content, "dump")

        assert result.rstrip("\n").endswith("[plugins]\ndump = true")
        assert "[cdf]" in result

    def test_does_not_touch_key_outside_plugins_section(self) -> None:
        content = "[other]\ndump = false\n\n[plugins]\ndev = false\n"

        result = _set_plugin_enabled(content, "dump")

        # The [other] section's dump must be left untouched; a new key added under [plugins].
        assert "[other]\ndump = false" in result
        assert "[plugins]\ndump = true\ndev = false" in result


class TestEnablePlugin:
    @pytest.mark.usefixtures("reset_cdf_toml_singleton")
    def test_enable_plugin_updates_file_and_resets_singleton(self, tmp_path: Path) -> None:
        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(
            '[cdf]\ndefault_env = "dev"\n\n[modules]\nversion = "0.0.0"\n\n[plugins]\ndump = false\n',
            encoding="utf-8",
        )
        # Prime the singleton so we can verify it is reset.
        cdf_toml_module._CDF_TOML = CDFToml.load(cwd=tmp_path, use_singleton=False)

        enabled = CDFToml.enable_plugin("dump", cwd=tmp_path)

        assert enabled is True
        assert cdf_toml_module._CDF_TOML is None
        reloaded = CDFToml.load(cwd=tmp_path, use_singleton=False)
        assert reloaded.plugins["dump"] is True

    def test_enable_plugin_returns_false_when_no_file(self, tmp_path: Path) -> None:
        assert CDFToml.enable_plugin("dump", cwd=tmp_path) is False
