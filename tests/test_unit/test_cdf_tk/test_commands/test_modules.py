from __future__ import annotations

import json
import zipfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests
import yaml
from _pytest.monkeypatch import MonkeyPatch
from questionary import Choice

from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildLineage
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ModelSyntaxWarning, Recommendation
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._lineage import ModuleLineageItem, ResourceLineageItem
from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.data_classes import ModuleLocation, Package, Packages
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from tests.data import COMPLETE_ORG, EXTERNAL_PACKAGE
from tests.test_unit.utils import MockQuestionary

COMPLETE_ORG_MODULES = COMPLETE_ORG / MODULES


@pytest.fixture(scope="session")
def selected_packages() -> Packages:
    return Packages.load(COMPLETE_ORG_MODULES)


@pytest.fixture(scope="session")
def selected_packages_location() -> Path:
    return COMPLETE_ORG_MODULES


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
    def test_modules_command(
        self, selected_packages: Packages, selected_packages_location: Path, tmp_path: Path
    ) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(
            organization_dir=target_path,
            selected_packages=selected_packages,
            environments=["dev"],
            mode=None,
            modules_source_path=selected_packages_location,
        )

        assert Path(target_path).exists()
        assert Path(target_path / "modules" / "my_example_module").exists()

    def test_modules_command_with_env(
        self, selected_packages: Packages, selected_packages_location: Path, tmp_path: Path
    ) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(
            organization_dir=target_path,
            selected_packages=selected_packages,
            environments=["dev", "prod"],
            mode=None,
            modules_source_path=selected_packages_location,
        )

        assert Path(target_path / "config.dev.yaml").exists()
        assert Path(target_path / "config.prod.yaml").exists()

    def test_config(self, selected_packages: Packages, selected_packages_location: Path, tmp_path: Path) -> None:
        assert selected_packages is not None

        target_path = tmp_path / "repo_root"

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(
            organization_dir=target_path,
            selected_packages=selected_packages,
            environments=["dev"],
            mode=None,
            modules_source_path=selected_packages_location,
        )

        config = yaml.safe_load(Path(target_path / "config.dev.yaml").read_text())
        assert config["variables"]["modules"]["my_example_module"]["var"] == "one"

    def test_config_external_modules(self, tmp_path: Path) -> None:
        target_path = tmp_path / "repo_root"

        selected_packages = Packages.load(EXTERNAL_PACKAGE)
        selected_packages_location = EXTERNAL_PACKAGE

        cmd = ModulesCommand(print_warning=True, skip_tracking=True)
        cmd._create(
            organization_dir=target_path,
            selected_packages=selected_packages,
            environments=["dev"],
            mode=None,
            modules_source_path=selected_packages_location,
        )

        config = yaml.safe_load(Path(target_path / "config.dev.yaml").read_text())
        assert config["variables"]["modules"]["shared_var"] == "shared"
        assert config["variables"]["modules"]["external_module_1"]["var"] == "one"
        assert config["variables"]["modules"]["external_module_2"]["var"] == "two"

    def test_adding(
        self, selected_packages: Packages, selected_packages_location: Path, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        target_path = tmp_path / "repo_root"
        cmd = ModulesCommand(print_warning=True, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES)

        first_batch = Packages({"small_1": selected_packages["small_1"]})
        second_batch = Packages({"small_2": selected_packages["small_2"]})

        cmd._create(
            organization_dir=target_path,
            selected_packages=first_batch,
            environments=["qa"],
            mode=None,
            modules_source_path=selected_packages_location,
        )
        with monkeypatch.context() as m:
            # Mocking questionary such that questionary.confirm.ask() returns True.
            questionary_mock = MagicMock()
            # MagicMock will always return other MagicMock objects
            # which when evaluated will return True.

            m.setattr("cognite_toolkit._cdf_tk.commands.modules.questionary", questionary_mock)
            cmd._create(
                organization_dir=target_path,
                selected_packages=second_batch,
                environments=["qa"],
                mode="update",
                modules_source_path=selected_packages_location,
            )

        config = yaml.safe_load(Path(target_path / "config.qa.yaml").read_text())
        assert config["variables"]["modules"]["my_example_module"]["var"] is not None
        assert (target_path / "modules" / "my_example_module").is_dir()

        assert config["variables"]["modules"]["my_file_expand_module"]["var"] is not None
        assert (target_path / "modules" / "my_file_expand_module").is_dir()

    def test_add_without_config_yaml(
        self, tmp_path: Path, monkeypatch: MonkeyPatch, modules_command_with_cached_download
    ) -> None:
        cmd = modules_command_with_cached_download(
            print_warning=True, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES
        )
        dummy_resource = "space: my_space"
        my_org = tmp_path / "my_org"
        modules = my_org / "modules"
        filepath = modules / "my_module" / "data_models" / "my.Space.yaml"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text(dummy_resource)

        def select_package(choices: list[Choice]) -> Package:
            selected_package = next((c for c in choices if "complete organization" in c.title.lower()), None)
            assert selected_package is not None
            return selected_package.value

        def select_first_module(choices: list[Choice]) -> list:
            selected_module = next((c for c in choices if "my_example_module" == c.title), None)
            assert selected_module is not None
            return [selected_module.value]

        answers = [select_package, select_first_module, False, False]

        with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers):
            cmd.add(my_org)

        yaml_file_count = len(list(modules.rglob("*.yaml")))

        assert yaml_file_count > 1, "Expected new yaml files to b created"

        def select_second_module(choices: list[Choice]) -> list:
            selected_module = next((c for c in choices if "my_file_expand_module" == c.title), None)
            assert selected_module is not None
            return [selected_module.value]

        answers = [select_package, select_second_module, False, False]

        with MockQuestionary(ModulesCommand.__module__, monkeypatch, answers):
            cmd.add(my_org)

        new_yaml_file_count = len(list(modules.rglob("*.yaml")))

        assert new_yaml_file_count > yaml_file_count, "Expected new yaml files to be created"

    def test_context_manager_scope(self):
        with ModulesCommand(module_source_dir=COMPLETE_ORG_MODULES) as cmd:
            first = Path(cmd._temp_download_dir / "test.txt")
            first.write_text("This is a test file.")
            assert first.exists()
        assert not first.exists(), "File should not exist after context manager exits"

    def test_download_success(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        dummy_file_content = b"PK\x05\x06\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"

        mock_response = MockResponse(dummy_file_content, status_code=200)
        monkeypatch.setattr(requests, "get", MagicMock(return_value=mock_response))

        cmd = ModulesCommand(print_warning=True, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES)
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
            ModulesCommand(module_source_dir=COMPLETE_ORG_MODULES)._download(test_url, output_path)

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
            ModulesCommand(module_source_dir=COMPLETE_ORG_MODULES)._download(test_url, output_path)

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
            ModulesCommand(module_source_dir=COMPLETE_ORG_MODULES)._unpack(output_path)

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
            ModulesCommand(module_source_dir=COMPLETE_ORG_MODULES)._unpack(output_path)

        assert isinstance(excinfo.value.__cause__, OSError)
        assert "No space left on device" in str(excinfo.value.__cause__)

    def test_download_deletes_existing_file(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test that _download method deletes existing zip files before downloading."""
        # Create a stale zip file that should be deleted
        stale_file_path = tmp_path / "test_file.zip"
        stale_content = b"stale content"
        stale_file_path.write_bytes(stale_content)

        # Verify the stale file exists
        assert stale_file_path.exists()
        assert stale_file_path.read_bytes() == stale_content

        # Mock the HTTP response with new content
        new_content = b"new content from download"
        mock_response = MockResponse(new_content, status_code=200)
        monkeypatch.setattr(requests, "get", MagicMock(return_value=mock_response))

        cmd = ModulesCommand(print_warning=True, skip_tracking=True, module_source_dir=COMPLETE_ORG / MODULES)

        # Call _download - this should delete the existing file and download new content
        cmd._download(url="http://example.com/test.zip", file_path=stale_file_path)

        # Verify the file was deleted and replaced with new content
        assert stale_file_path.exists()
        assert stale_file_path.read_bytes() == new_content
        assert stale_file_path.read_bytes() != stale_content

        # Verify the HTTP request was made
        requests.get.assert_called_once_with("http://example.com/test.zip", stream=True)

    def test_iterate_modules_finds_modules_in_temp_download_dir(self, tmp_path: Path) -> None:
        """Test that iterate_modules can find modules in the _temp_download_dir.

        This test verifies that when modules are downloaded to the temporary directory,
        the iterate_modules function can properly discover and iterate over them.
        The test creates a mock module structure with the required resource directories
        (like 'data_models') that the module discovery logic recognizes.
        """
        from cognite_toolkit._cdf_tk.utils.modules import iterate_modules

        cmd = ModulesCommand(print_warning=True, skip_tracking=True, module_source_dir=COMPLETE_ORG / MODULES)

        # Create a mock module structure in the temp download directory
        # This simulates what would happen when modules are downloaded
        mock_module_dir = cmd._temp_download_dir / "test_module"
        mock_module_dir.mkdir(parents=True, exist_ok=True)

        # Create a resource directory (e.g., 'data_models') that LOADER_BY_FOLDER_NAME recognizes
        # This is required for the module to be identified as a valid module
        resource_dir = mock_module_dir / "data_models"
        resource_dir.mkdir()

        # Create a sample file in the resource directory
        sample_file = resource_dir / "sample.yaml"
        sample_file.write_text("test content")

        # Now test that iterate_modules can find this module
        modules_found = list(iterate_modules(cmd._temp_download_dir))

        # Should find at least one module
        assert len(modules_found) > 0, f"Expected to find modules in {cmd._temp_download_dir}"

        # Verify the module structure
        module_dir, files = modules_found[0]
        assert module_dir == mock_module_dir
        assert len(files) > 0
        assert any(file.name == "sample.yaml" for file in files)

        # Clean up
        import shutil

        shutil.rmtree(mock_module_dir)

    def test_list_json_output_is_parseable(self, tmp_path: Path, monkeypatch: MonkeyPatch, capsys) -> None:
        lineage = _module_list_lineage(tmp_path)
        _patch_modules_list(monkeypatch, lineage)

        cmd = ModulesCommand(print_warning=False, skip_tracking=True)
        cmd.list(organization_dir=tmp_path, build_env_name="dev", output_format="json")

        payload = json.loads(capsys.readouterr().out)
        assert payload == {
            "environment": "dev",
            "insights_summary": {
                ModelSyntaxWarning.__name__: 1,
                Recommendation.__name__: 2,
            },
            "modules": [
                {
                    "build_result": "Success",
                    "insights": {
                        ModelSyntaxWarning.__name__: 1,
                        Recommendation.__name__: 2,
                    },
                    "location": "modules/my_module",
                    "module_name": "my_module",
                    "resource_folders": 2,
                    "resources": 3,
                    "syntax_warnings": 1,
                }
            ],
            "modules_summary": {"failed": 0, "processed": 1, "succeeded": 1},
            "organization_dir": tmp_path.as_posix(),
        }

    def test_list_table_shows_insight_types(self, tmp_path: Path, monkeypatch: MonkeyPatch, capsys) -> None:
        lineage = _module_list_lineage(tmp_path)
        _patch_modules_list(monkeypatch, lineage)

        cmd = ModulesCommand(print_warning=False, skip_tracking=True)
        cmd.list(organization_dir=tmp_path, build_env_name="dev", output_format="table")

        output = capsys.readouterr().out
        assert "my_module" in output
        assert "Syntax warnings" in output
        assert ModelSyntaxWarning.__name__ in output
        assert Recommendation.__name__ in output
        assert "SUCCESS" in output
        assert "modules/my_module" in output

    def test_list_passes_config_yaml_to_tmp_build(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        config_yaml = tmp_path / "config.dev.yaml"
        config_yaml.write_text("environment:\n  name: dev\n")
        captured: dict[str, Path | None] = {}

        def fake_tmp_build(
            self: BuildV2Command,
            organization_dir: Path,
            config_yaml: Path | None = None,
            client: object = None,
        ) -> BuildLineage:
            captured["organization_dir"] = organization_dir
            captured["config_yaml"] = config_yaml
            return _module_list_lineage(tmp_path)

        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.modules.verify_module_directory", lambda *_: None)
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.modules.BuildV2Command.tmp_build", fake_tmp_build)

        cmd = ModulesCommand(print_warning=False, skip_tracking=True)
        cmd.list(organization_dir=tmp_path, build_env_name="dev", output_format="json")

        assert captured == {"organization_dir": tmp_path, "config_yaml": config_yaml}

    @pytest.fixture
    def lookup_packages(self, tmp_path: Path) -> Packages:
        """Minimal Packages fixture for _find_and_select_module tests."""
        base = tmp_path
        mod_a = ModuleLocation(dir=base / "mod_a", source_absolute_path=base, source_paths=[])
        mod_b = ModuleLocation(dir=base / "mod_b", source_absolute_path=base, source_paths=[])
        mod_locked = ModuleLocation(dir=base / "mod_locked", source_absolute_path=base, source_paths=[])
        mod_only_fixed = ModuleLocation(dir=base / "mod_only_fixed", source_absolute_path=base, source_paths=[])
        # A second cherry-pickable package that also contains a module named "mod_b" (collision)
        mod_b_alt = ModuleLocation(dir=base / "alt" / "mod_b", source_absolute_path=base, source_paths=[])
        mod_c = ModuleLocation(dir=base / "mod_c", source_absolute_path=base, source_paths=[])
        return Packages(
            {
                "cherry_pkg": Package(
                    name="cherry_pkg", title="Cherry Package", can_cherry_pick=True, modules=[mod_a, mod_b]
                ),
                "fixed_pkg": Package(
                    name="fixed_pkg",
                    title="Fixed Package",
                    can_cherry_pick=False,
                    modules=[mod_locked, mod_only_fixed],
                ),
                "other_cherry_pkg": Package(
                    name="other_cherry_pkg",
                    title="Other Cherry Package",
                    can_cherry_pick=True,
                    modules=[mod_b_alt, mod_c],
                ),
            }
        )

    @pytest.mark.parametrize(
        "name, expected_pkg, expected_modules",
        [
            ("cherry_pkg", "cherry_pkg", {"mod_a", "mod_b"}),
            ("CHERRY_PKG", "cherry_pkg", {"mod_a", "mod_b"}),
            ("fixed_pkg", "fixed_pkg", {"mod_locked", "mod_only_fixed"}),
            ("other_cherry_pkg", "other_cherry_pkg", {"mod_b", "mod_c"}),
            ("mod_a", "cherry_pkg", {"mod_a"}),
            ("MOD_A", "cherry_pkg", {"mod_a"}),
            ("mod_c", "other_cherry_pkg", {"mod_c"}),
            ("cherry_pkg:mod_b", "cherry_pkg", {"mod_b"}),
            ("other_cherry_pkg:mod_b", "other_cherry_pkg", {"mod_b"}),
            ("CHERRY_PKG:MOD_B", "cherry_pkg", {"mod_b"}),
        ],
    )
    def test_find_and_select_module_lookup(
        self, lookup_packages: Packages, name: str, expected_pkg: str, expected_modules: set[str]
    ) -> None:
        cmd = ModulesCommand(print_warning=False, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES)
        result = cmd._find_and_select_module(lookup_packages, name, [])
        assert expected_pkg in result
        assert {m.name for m in result[expected_pkg].modules} == expected_modules

    def test_find_and_select_module_skips_installed(self, lookup_packages: Packages) -> None:
        cmd = ModulesCommand(print_warning=False, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES)
        result = cmd._find_and_select_module(lookup_packages, "cherry_pkg", ["mod_a"])
        assert len(result["cherry_pkg"].modules) == 1
        assert result["cherry_pkg"].modules[0].name == "mod_b"

    @pytest.mark.parametrize(
        "name, existing, match",
        [
            ("cherry_pkg", ["mod_a", "mod_b"], "already installed"),
            ("mod_a", ["mod_a"], "already installed"),
            ("mod_locked", [], "not found"),  # non-cherry-pickable module
            ("mod_b", [], "multiple packages"),  # ambiguous: exists in cherry_pkg and other_cherry_pkg
            ("cherry_pk", [], "Did you mean"),  # typo
            ("zzz_completely_unrelated_xyz", [], "not found"),
            ("cherry_pkg:mod_b", ["mod_b"], "already installed"),
            ("no_such_pkg:mod_a", [], "Package 'no_such_pkg' not found"),  # unknown package prefix
            ("cherry_pkg:no_such_mod", [], "Module 'no_such_mod' not found in package 'cherry_pkg'"),
            ("cherry_pkg:", [], "Invalid syntax"),  # missing module part
            (":mod_a", [], "Invalid syntax"),  # missing package part
            ("fixed_pkg:mod_locked", [], "does not support cherry-picking"),  # non-cherry-pickable package
            ("dp:contextualization:cdf_entity_matching", [], "Invalid syntax"),  # more than one ':'
            ("cherry_pk:mod_a", [], "Did you mean 'cherry_pkg'"),  # package typo, close match suggested
            ("cherry_pkg:mod_bb", [], "Did you mean 'mod_b'"),  # module typo, close match suggested
        ],
    )
    def test_find_and_select_module_errors(
        self, lookup_packages: Packages, name: str, existing: list[str], match: str
    ) -> None:
        cmd = ModulesCommand(print_warning=False, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES)
        with pytest.raises(ToolkitError, match=match):
            cmd._find_and_select_module(lookup_packages, name, existing)

    def test_add_with_deployment_pack(
        self, lookup_packages: Packages, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        my_org = tmp_path / "my_org"
        stub_file = my_org / "modules" / "stub" / "data_models" / "stub.Space.yaml"
        stub_file.parent.mkdir(parents=True, exist_ok=True)
        stub_file.write_text("space: stub_space")

        cmd = ModulesCommand(print_warning=False, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES)
        monkeypatch.setattr(cmd, "_get_available_packages", lambda: (lookup_packages, COMPLETE_ORG_MODULES))
        monkeypatch.setattr(cmd, "_get_download_data", lambda _: False)

        captured: dict = {}

        def capture_create(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(cmd, "_create", capture_create)
        cmd.add(my_org, deployment_pack="mod_a")

        assert "selected_packages" in captured, "_create was not called"
        assert "cherry_pkg" in captured["selected_packages"]
        assert len(captured["selected_packages"]["cherry_pkg"].modules) == 1
        assert captured["selected_packages"]["cherry_pkg"].modules[0].name == "mod_a"

    def test_add_with_deployment_pack_invalid_name_raises(
        self, lookup_packages: Packages, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        my_org = tmp_path / "my_org"
        stub_file = my_org / "modules" / "stub" / "data_models" / "stub.Space.yaml"
        stub_file.parent.mkdir(parents=True, exist_ok=True)
        stub_file.write_text("space: stub_space")

        cmd = ModulesCommand(print_warning=False, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES)
        monkeypatch.setattr(cmd, "_get_available_packages", lambda: (lookup_packages, COMPLETE_ORG_MODULES))

        with pytest.raises(ToolkitError, match="not found"):
            cmd.add(my_org, deployment_pack="nonexistent_module")

    def test_add_with_deployment_pack_package_prefix_disambiguates(
        self, lookup_packages: Packages, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        my_org = tmp_path / "my_org"
        stub_file = my_org / "modules" / "stub" / "data_models" / "stub.Space.yaml"
        stub_file.parent.mkdir(parents=True, exist_ok=True)
        stub_file.write_text("space: stub_space")

        cmd = ModulesCommand(print_warning=False, skip_tracking=True, module_source_dir=COMPLETE_ORG_MODULES)
        monkeypatch.setattr(cmd, "_get_available_packages", lambda: (lookup_packages, COMPLETE_ORG_MODULES))
        monkeypatch.setattr(cmd, "_get_download_data", lambda _: False)

        captured: dict = {}

        def capture_create(**kwargs: object) -> None:
            captured.update(kwargs)

        monkeypatch.setattr(cmd, "_create", capture_create)
        cmd.add(my_org, deployment_pack="other_cherry_pkg:mod_b")

        assert "selected_packages" in captured, "_create was not called"
        assert "other_cherry_pkg" in captured["selected_packages"]
        assert len(captured["selected_packages"]["other_cherry_pkg"].modules) == 1
        assert captured["selected_packages"]["other_cherry_pkg"].modules[0].name == "mod_b"


def _patch_modules_list(monkeypatch: MonkeyPatch, lineage: BuildLineage) -> None:
    monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.modules.verify_module_directory", lambda *_: None)
    monkeypatch.setattr(
        "cognite_toolkit._cdf_tk.commands.modules.BuildV2Command.tmp_build",
        lambda self, organization_dir, config_yaml=None, client=None: lineage,
    )


def _module_list_lineage(tmp_path: Path) -> BuildLineage:
    module_path = tmp_path / "modules" / "my_module"
    module_path.mkdir(parents=True, exist_ok=True)
    source_dir = module_path / "data_models"
    source_dir.mkdir(parents=True, exist_ok=True)
    build_dir = tmp_path / "build" / "data_models"
    build_dir.mkdir(parents=True, exist_ok=True)

    views = [
        ResourceLineageItem(
            source_file=source_dir / f"view_{index}.View.yaml",
            source_hash="abc",
            type={"resource_folder": "data_models", "kind": "View"},
            built_file=build_dir / f"{index}-view.View.yaml",
            identifier={"space": "my_space", "externalId": f"View{index}", "version": "1"},
        )
        for index in (1, 2)
    ]
    files_dir = module_path / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    files_build = tmp_path / "build" / "files"
    files_build.mkdir(parents=True, exist_ok=True)
    file_item = ResourceLineageItem(
        source_file=files_dir / "my_file.FileMetadata.yaml",
        source_hash="def",
        type={"resource_folder": "files", "kind": "FileMetadata"},
        built_file=files_build / "1-my_file.FileMetadata.yaml",
        identifier={"externalId": "my_file"},
    )

    return BuildLineage(
        organization_dir=tmp_path,
        build_dir=tmp_path / "build",
        modules_summary={"processed": 1, "succeeded": 1, "failed": 0},
        insights_summary={
            ModelSyntaxWarning.__name__: 1,
            Recommendation.__name__: 2,
        },
        module_lineage=[
            ModuleLineageItem(
                module_id="modules/my_module",
                module_path=module_path,
                insights_summary={
                    ModelSyntaxWarning.__name__: 1,
                    Recommendation.__name__: 2,
                },
                resource_lineage=[*views, file_item],
            )
        ],
    )
