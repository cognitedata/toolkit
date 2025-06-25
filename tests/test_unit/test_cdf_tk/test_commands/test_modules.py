from __future__ import annotations

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

    def test_download_and_unpack_success_zip(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        cmd = ModulesCommand(print_warning=True, skip_tracking=True)

        dummy_zip_content = b"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        mock_response = MockResponse(dummy_zip_content, status_code=200)
        monkeypatch.setattr(requests, "get", MagicMock(return_value=mock_response))

        mock_zip_file_instance = MagicMock(spec=zipfile.ZipFile)
        monkeypatch.setattr(zipfile, "ZipFile", MagicMock(return_value=mock_zip_file_instance))

        # Simulate the zip extraction by creating a dummy file in the target directory
        mock_zip_file_instance.__enter__.return_value.extractall.side_effect = lambda path: (
            Path(path) / "extracted_content.txt"
        ).touch()

        output_zip_path = tmp_path / "test_file.zip"
        extracted_file_path = tmp_path / "extracted_content.txt"

        cmd._download_and_unpack("http://example.com/test.zip", output_zip_path)

        requests.get.assert_called_once_with("http://example.com/test.zip", stream=True)
        assert mock_response.raise_for_status_called

        assert output_zip_path.exists()
        assert output_zip_path.read_bytes() == dummy_zip_content

        zipfile.ZipFile.assert_called_once_with(output_zip_path, "r")
        mock_zip_file_instance.__enter__.return_value.extractall.assert_called_once_with(output_zip_path.parent)

        assert extracted_file_path.exists()

    @pytest.mark.parametrize(
        "test_id, mock_setup_func, expected_error_msg_part, expected_chained_exception_type, output_file_should_exist, url_suffix",
        [
            (
                "http_error",
                lambda monkeypatch, url, output_path: monkeypatch.setattr(
                    requests, "get", MagicMock(return_value=MockResponse(b"", status_code=404))
                ),
                "Error downloading file from",
                requests.exceptions.HTTPError,
                False,
                "error.zip",
            ),
            (
                "network_error",
                lambda monkeypatch, url, output_path: monkeypatch.setattr(
                    requests, "get", MagicMock(side_effect=requests.exceptions.RequestException("Connection aborted."))
                ),
                "Error downloading file from",
                requests.exceptions.RequestException,
                False,
                "network_error.zip",
            ),
            (
                "corrupt_zip_error",
                lambda monkeypatch, url, output_path: (
                    monkeypatch.setattr(
                        requests, "get", MagicMock(return_value=MockResponse(b"corrupt", status_code=200))
                    ),
                    monkeypatch.setattr(
                        zipfile, "ZipFile", MagicMock(side_effect=zipfile.BadZipFile("File is not a zip file"))
                    ),
                ),
                "Error unpacking zip file",
                zipfile.BadZipFile,
                True,  # File is downloaded before zip error
                "corrupt.zip",
            ),
            (
                "os_error_during_write",
                lambda monkeypatch, url, output_path: (
                    monkeypatch.setattr(
                        requests, "get", MagicMock(return_value=MockResponse(b"some content", status_code=200))
                    ),
                    monkeypatch.setattr("builtins.open", MagicMock(side_effect=OSError("No space left on device"))),
                ),
                "An unexpected error occurred during download/unpack of",
                OSError,
                False,
                "os_error_file.txt",
            ),
        ],
        ids=["http_error", "network_error", "corrupt_zip", "os_error_write"],
    )
    def test_download_and_unpack_exception_handling(
        self,
        tmp_path: Path,
        monkeypatch: MonkeyPatch,
        test_id: str,
        mock_setup_func,
        expected_error_msg_part: str,
        expected_chained_exception_type: type,
        output_file_should_exist: bool,
        url_suffix: str,
    ) -> None:
        test_url = f"[http://example.com/](http://example.com/){url_suffix}"
        output_path = tmp_path / url_suffix

        mock_setup_func(monkeypatch, test_url, output_path)

        with pytest.raises(ToolkitError) as excinfo:
            ModulesCommand()._download_and_unpack(test_url, output_path)

        assert expected_error_msg_part in str(excinfo.value)
        assert isinstance(excinfo.value.__cause__, expected_chained_exception_type)

        if output_file_should_exist:
            assert output_path.exists()
        else:
            assert not output_path.exists()

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

    def test_library_invalid_checksum_raises(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        test_url = "[http://example.com/](http://example.com/)corrupt_checksum.zip"
        output_path = tmp_path / "corrupt_checksum.zip"

        dummy_zip_content = b"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"
        mock_response = MockResponse(dummy_zip_content, status_code=200)
        monkeypatch.setattr(requests, "get", MagicMock(return_value=mock_response))

        mock_zip_file_instance = MagicMock(spec=zipfile.ZipFile)
        monkeypatch.setattr(zipfile, "ZipFile", MagicMock(return_value=mock_zip_file_instance))
        mock_zip_file_instance.__enter__.return_value.extractall.side_effect = lambda path: (
            Path(path) / "extracted_content.txt"
        ).touch()

        with ModulesCommand() as cmd:
            with pytest.raises(ToolkitError, match="Checksum mismatch"):
                cmd._download_and_unpack(
                    test_url,
                    output_path,
                    checksum="abc1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                )
