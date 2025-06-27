from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
import yaml
from _pytest.monkeypatch import MonkeyPatch
from questionary import Choice

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH
from cognite_toolkit._cdf_tk.data_classes import Package, Packages
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from tests.test_unit.utils import MockQuestionary


@pytest.fixture(scope="session")
def selected_packages() -> Packages:
    return Packages.load(BUILTIN_MODULES_PATH)


class MockResponse:
    def __init__(self, content, status_code=200, headers=None):
        self._content = content
        self.status_code = status_code
        self.headers = headers if headers is not None else {"content-length": str(len(content))}
        self.raise_for_status_called = False

    def iter_content(self, chunk_size=8192):
        yield self._content

    def raise_for_status(self):
        self.raise_for_status_called = True
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP Error: {self.status_code}")


class TestModulesCommand:
    def test_modules_command(self, selected_packages: Packages, tmp_path: Path) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(organization_dir=target_path, selected_packages=selected_packages, environments=["dev"], mode=None)

        assert Path(target_path).exists()
        assert Path(target_path / "modules" / "infield" / "cdf_infield_common").exists()

    def test_modules_command_with_env(self, selected_packages: Packages, tmp_path: Path) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(
            organization_dir=target_path, selected_packages=selected_packages, environments=["dev", "prod"], mode=None
        )

        assert Path(target_path / "config.dev.yaml").exists()
        assert Path(target_path / "config.prod.yaml").exists()

    def test_config(self, selected_packages: Packages, tmp_path: Path) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(organization_dir=target_path, selected_packages=selected_packages, environments=["dev"], mode=None)

        config = yaml.safe_load(Path(target_path / "config.dev.yaml").read_text())
        assert config["variables"]["modules"]["infield"]["first_location"] == "oid"

    def test_adding(self, selected_packages: Packages, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        target_path = tmp_path / "repo_root"
        cmd = ModulesCommand(print_warning=True, skip_tracking=True)

        first_batch = Packages({"infield": selected_packages["infield"]})
        second_batch = Packages({"quickstart": selected_packages["inrobot"]})

        cmd._create(organization_dir=target_path, selected_packages=first_batch, environments=["qa"], mode=None)
        with monkeypatch.context() as m:
            # Mocking questionary such that questionary.confirm.ask() returns True.
            questionary_mock = MagicMock()
            # MagicMock will always return other MagicMock objects
            # which when evaluated will return True.

            m.setattr("cognite_toolkit._cdf_tk.commands.modules.questionary", questionary_mock)
            cmd._create(
                organization_dir=target_path, selected_packages=second_batch, environments=["qa"], mode="update"
            )

        config = yaml.safe_load(Path(target_path / "config.qa.yaml").read_text())
        assert config["variables"]["modules"]["infield"]["first_location"] is not None
        assert (target_path / "modules" / "infield" / "cdf_infield_common").is_dir()

        assert config["variables"]["modules"]["inrobot"]["first_location"] is not None
        assert (target_path / "modules" / "inrobot" / "cdf_inrobot_common").is_dir()

    def test_add_without_config_yaml(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        dummy_resource = "space: my_space"
        my_org = tmp_path / "my_org"
        moules = my_org / "modules"
        filepath = moules / "my_module" / "data_models" / "my.Space.yaml"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(dummy_resource)

        def select_source_system(choices: list[Choice]) -> Package:
            selected_package = next((c for c in choices if "sourcesystem" in c.title.lower()), None)
            assert selected_package is not None
            return selected_package.value

        def select_sap_events(choices: list[Choice]) -> list:
            selected_module = next(
                (c for c in choices if "sap" in c.title.lower() and "event" in c.title.lower()), None
            )
            assert selected_module is not None
            return [selected_module.value]

        answers = [select_source_system, select_sap_events, False, False]

        with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers):
            cmd.add(my_org)

        yaml_file_count = len(list(moules.rglob("*.yaml")))

        assert yaml_file_count > 1, "Expected new yaml files to b created"

        def select_sap_assets(choices: list[Choice]) -> list:
            selected_module = next(
                (c for c in choices if "sap" in c.title.lower() and "asset" in c.title.lower()), None
            )
            assert selected_module is not None
            return [selected_module.value]

        answers = [select_source_system, select_sap_assets, False, False]

        with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers):
            cmd.add(my_org)

        new_yaml_file_count = len(list(moules.rglob("*.yaml")))

        assert new_yaml_file_count > yaml_file_count, "Expected new yaml files to be created"

    def test_context_manager_scope(self):
        with ModulesCommand() as cmd:
            first = Path(cmd._temp_download_dir / "test.txt")
            first.write_text("This is a test file.")
            assert first.exists()
        assert not first.exists(), "File should not exist after context manager exits"

    @pytest.fixture(autouse=True)
    def reset_cdf_toml_singleton(self):
        global _CDF_TOML
        _CDF_TOML = None
        yield
        _CDF_TOML = None

    def test_library_fallback_if_flag_is_false(self, tmp_path: Path) -> None:
        valid_toml_content = """
        [cdf]
        [modules]
        version = "0.0.0"
        [alpha_flags]
        external_libraries = false
        [library.valid_url]
        url = "https://github.com/cognitedata/package.zip"
        """

        file_path = tmp_path / CDFToml.file_name
        file_path.write_text(valid_toml_content)

        with ModulesCommand() as cmd:
            packs = cmd._get_available_packages()
            assert "quickstart" in packs

    def test_download_success(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        dummy_file_content = b"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"

        mock_response = MockResponse(dummy_file_content, status_code=200)
        monkeypatch.setattr(requests, "get", MagicMock(return_value=mock_response))

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        output_zip_path = tmp_path / "test_file.zip"

        cmd._download(url="http://example.com/test.zip", file_path=output_zip_path)

        requests.get.assert_called_once_with("http://example.com/test.zip", stream=True)
        assert output_zip_path.exists()
        assert output_zip_path.read_bytes() == dummy_file_content

    def test_download_errors_http_error(
        self,
        tmp_path: Path,
        monkeypatch: MonkeyPatch,
    ) -> None:
        test_url = "http://example.com/test_file.zip"
        output_path = tmp_path / "test_file.zip"

        # Arrange: Mock requests.get to return a MockResponse with a 404 status
        monkeypatch.setattr(requests, "get", MagicMock(return_value=MockResponse(b"", status_code=404)))

        # Act & Assert
        with pytest.raises(ToolkitError) as excinfo:
            ModulesCommand()._download(test_url, output_path)

        assert isinstance(excinfo.value.__cause__, requests.exceptions.HTTPError)

    def test_download_errors_request_exception(
        self,
        tmp_path: Path,
        monkeypatch: MonkeyPatch,
    ) -> None:
        test_url = "http://example.com/test_file.zip"
        output_path = tmp_path / "test_file.zip"

        # Arrange: Mock requests.get to raise a RequestException directly
        monkeypatch.setattr(
            requests, "get", MagicMock(side_effect=requests.exceptions.RequestException("Connection aborted."))
        )

        # Act & Assert
        with pytest.raises(ToolkitError) as excinfo:
            ModulesCommand()._download(test_url, output_path)

        assert "Error downloading file" in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, requests.exceptions.RequestException)
        assert "Connection aborted." in str(excinfo.value.__cause__)

    def test_unpack_errors_bad_zip_file(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        url_suffix = "corrupt_file.zip"
        output_path = tmp_path / url_suffix

        output_path.touch()

        mock_zipfile_instance = MagicMock()
        mock_zipfile_instance.__enter__.side_effect = zipfile.BadZipFile("File is not a zip file")

        monkeypatch.setattr(zipfile, "ZipFile", MagicMock(return_value=mock_zipfile_instance))

        with pytest.raises(ToolkitError) as excinfo:
            ModulesCommand()._unpack(output_path)

        assert isinstance(excinfo.value.__cause__, zipfile.BadZipFile)
        assert "File is not a zip file" in str(excinfo.value.__cause__)

    def test_unpack_errors_os_error_during_write(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        url_suffix = "valid_archive.zip"
        output_path = tmp_path / url_suffix

        with zipfile.ZipFile(output_path, "w") as zf:
            zf.writestr("dummy_file.txt", "content")

        mock_zipfile_ref = MagicMock()
        mock_zipfile_ref.extractall.side_effect = OSError("No space left on device")
        mock_zipfile_ref.__enter__.return_value = mock_zipfile_ref
        mock_zipfile_ref.__exit__.return_value = None

        monkeypatch.setattr(zipfile, "ZipFile", MagicMock(return_value=mock_zipfile_ref))

        with pytest.raises(ToolkitError) as excinfo:
            ModulesCommand()._unpack(output_path)

        assert isinstance(excinfo.value.__cause__, OSError)
        assert "No space left on device" in str(excinfo.value.__cause__)

    def test_checksum_format(self, tmp_path: Path) -> None:
        invalid_checksum = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"
        with pytest.raises(ToolkitError) as excinfo:
            ModulesCommand()._validate_checksum(invalid_checksum, Path(tmp_path / "test_file.zip"))

        assert "Unsupported checksum format" in str(excinfo.value)

    def test_checksum_success(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        file_path = tmp_path / "test_file.zip"
        dummy_file_content = b"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        file_path.write_bytes(dummy_file_content)

        expected_checksum = f"sha256:{hashlib.sha256(dummy_file_content).hexdigest()}"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        try:
            cmd._validate_checksum(
                checksum=expected_checksum,
                file_path=file_path,  # Pass the correct Path object
            )
        except ToolkitError as e:
            pytest.fail(f"'_validate_checksum' raised an unexpected ToolkitError: {e}")
