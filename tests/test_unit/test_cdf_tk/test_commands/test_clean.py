from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk import cdf_toml
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import BuildCommand, CleanCommand

# For other flags, use the real implementation
from cognite_toolkit._cdf_tk.constants import BUILD_ENVIRONMENT_FILE
from cognite_toolkit._cdf_tk.data_classes._config_yaml import BuildEnvironment
from cognite_toolkit._cdf_tk.utils import read_yaml_file
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


@pytest.fixture
def build_environment(
    build_tmp_path: Path,
    complete_org_dir: Path,
    env_vars_with_client: EnvironmentVariables,
) -> BuildEnvironment:
    BuildCommand(silent=True, skip_tracking=True).execute(
        verbose=False,
        organization_dir=complete_org_dir,
        build_dir=build_tmp_path,
        selected=None,
        no_clean=False,
        client=env_vars_with_client.get_client(),
        build_env_name="dev",
        on_error="raise",
    )
    return BuildEnvironment.load(read_yaml_file(build_tmp_path / BUILD_ENVIRONMENT_FILE), "dev", "build")


@pytest.fixture
def cdf_toml_v07_disabled(monkeypatch: MonkeyPatch):
    """Fixture that modifies CDFToml singleton to disable v07 flag."""
    my_cdf_toml = CDFToml.load(use_singleton=False)
    my_cdf_toml.alpha_flags["v07"] = False
    monkeypatch.setattr(cdf_toml, "_CDF_TOML", my_cdf_toml)
    yield my_cdf_toml
    # Cleanup: reset singleton
    monkeypatch.setattr(cdf_toml, "_CDF_TOML", None)


@pytest.fixture
def cdf_toml_v07_enabled(monkeypatch: MonkeyPatch):
    """Fixture that modifies CDFToml singleton to enable v07 flag."""
    my_cdf_toml = CDFToml.load(use_singleton=False)
    my_cdf_toml.alpha_flags["v07"] = True
    monkeypatch.setattr(cdf_toml, "_CDF_TOML", my_cdf_toml)
    yield my_cdf_toml
    # Cleanup: reset singleton
    monkeypatch.setattr(cdf_toml, "_CDF_TOML", None)


class TestCleanCommandSelectModules:
    """Test the CleanCommand._select_modules method."""

    def test_select_modules_not_v07_returns_all_modules(
        self,
        build_environment: BuildEnvironment,
        cdf_toml_v07_disabled: CDFToml,
    ) -> None:
        """Test that when v07 flag is disabled, all modules are returned."""
        cmd = CleanCommand(silent=True, skip_tracking=True)
        result = cmd._select_modules(build_environment, module_str=None)

        # Should return all built_modules when v07 is disabled
        assert result == build_environment.read_modules

    def test_select_specific_module(
        self,
        build_environment: BuildEnvironment,
        cdf_toml_v07_enabled: CDFToml,
    ) -> None:
        cmd = CleanCommand(silent=True, skip_tracking=True)
        result = cmd._select_modules(build_environment, module_str="my_example_module")

        assert len(result) == 1
        assert result[0].name == "my_example_module"

    def test_select_no_such_module(
        self,
        build_environment: BuildEnvironment,
        cdf_toml_v07_enabled: CDFToml,
    ) -> None:
        cmd = CleanCommand(silent=True, skip_tracking=True)
        result = cmd._select_modules(build_environment, module_str="no_such_module")
        assert len(result) == 0

    def test_select_interactive_selection_with_questionary_response(
        self,
        build_environment: BuildEnvironment,
        cdf_toml_v07_enabled: CDFToml,
    ) -> None:
        """Test interactive module selection with different questionary responses."""
        cmd = CleanCommand(silent=True, skip_tracking=True)

        # Mock questionary.checkbox to return a mock object whose ask() method returns the selected modules
        mock_checkbox = MagicMock()
        mock_checkbox.ask.return_value = [build_environment.read_modules[0], build_environment.read_modules[1]]

        with patch("cognite_toolkit._cdf_tk.commands.clean.questionary.checkbox", return_value=mock_checkbox):
            result = cmd._select_modules(build_environment, module_str=None)

        assert len(result) == 2
        assert result[0].dir.name == "populate_model"
        assert result[1].dir.name == "my_file_expand_module"
