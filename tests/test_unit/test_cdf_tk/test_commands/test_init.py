from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit import _version
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml, CLIConfig, ModulesConfig
from cognite_toolkit._cdf_tk.commands.init import InitCommand
from cognite_toolkit._cdf_tk.commands.repo import RepoCommand
from tests.test_unit.utils import MockQuestionary


# Reset singleton before each test to ensure test isolation
@pytest.fixture(autouse=True)
def reset_cdf_toml_singleton():
    """Reset CDFToml singleton before and after each test to ensure test isolation."""
    global _CDF_TOML
    _CDF_TOML = None
    yield
    _CDF_TOML = None  # Clean up after test as well


class TestInitCommand:
    @staticmethod
    def _mock_cdf_toml_load_non_loaded(cls, cwd=None, use_singleton=True):
        """Mock CDFToml.load to return a non-loaded instance (as if cdf.toml doesn't exist)."""
        cwd = cwd or Path.cwd()
        return CDFToml(
            cdf=CLIConfig(cwd),
            modules=ModulesConfig.load({"version": _version.__version__}),
            alpha_flags={},
            plugins={},
            libraries={},
            is_loaded_from_file=False,
        )

    def test_cdf_toml_is_always_created(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test that cdf.toml is always created when running init."""
        cdf_toml_path = tmp_path / "cdf.toml"
        if cdf_toml_path.exists():
            cdf_toml_path.unlink()

        with monkeypatch.context() as m:
            m.chdir(tmp_path)

            user_input = [
                "initToml",  # Select: "Create toml file"
                "",  # Text: organization_dir prompt (empty = current directory)
                "__exit__",  # Select: Exit
            ]
            with (
                patch.object(CDFToml, "load", classmethod(self._mock_cdf_toml_load_non_loaded)),
                MockQuestionary(
                    [
                        "cognite_toolkit._cdf_tk.commands.init",
                        "cognite_toolkit._cdf_tk.commands.modules",
                    ],
                    monkeypatch,
                    user_input,
                ),
            ):
                cmd = InitCommand(print_warning=False, skip_tracking=True)
                cmd.execute(emulate_dot_seven=True)

        assert cdf_toml_path.exists(), "cdf.toml should be created"
        assert CDFToml.load(tmp_path, use_singleton=False).is_loaded_from_file

    def test_github_folder_created_when_selecting_repo_and_github(
        self, tmp_path: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Test that selecting repo and GitHub creates a .github folder."""
        with monkeypatch.context() as m:
            m.chdir(tmp_path)

            user_input = [
                "initToml",  # Select: "Create toml file"
                "",  # Text: organization_dir prompt (empty = current directory)
                "initRepo",  # Select: "Git repository"
                "GitHub",  # Select: GitHub hosting (from repo module)
                "__exit__",  # Select: Exit
            ]
            with (
                patch.object(CDFToml, "load", classmethod(self._mock_cdf_toml_load_non_loaded)),
                MockQuestionary(
                    [
                        "cognite_toolkit._cdf_tk.commands.init",
                        "cognite_toolkit._cdf_tk.commands.modules",
                        "cognite_toolkit._cdf_tk.commands.repo",
                    ],
                    monkeypatch,
                    user_input,
                ),
            ):
                # Patch _init_repo to use RepoCommand with skip_git_verify
                def mock_init_repo(self, dry_run: bool = False) -> None:
                    if dry_run:
                        return
                    repo_command = RepoCommand(skip_git_verify=True, print_warning=False, skip_tracking=True)
                    repo_command.run(lambda: repo_command.init(cwd=Path.cwd(), host=None, verbose=False))

                with patch.object(InitCommand, "_init_repo", mock_init_repo):
                    cmd = InitCommand(print_warning=False, skip_tracking=True)
                    cmd.execute(emulate_dot_seven=True)

        # Verify .github folder was created
        github_folder = tmp_path / ".github" / "workflows"
        assert github_folder.exists(), ".github/workflows folder should be created"
        assert (github_folder / "deploy.yaml").exists() and (github_folder / "dry-run.yaml").exists()

    def test_exit_actually_exits(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        """Test that selecting exit actually exits the command."""
        with monkeypatch.context() as m:
            m.chdir(tmp_path)
            user_input = ["__exit__"]
            with MockQuestionary(
                ["cognite_toolkit._cdf_tk.commands.init"],
                monkeypatch,
                user_input,
            ):
                cmd = InitCommand(print_warning=False, skip_tracking=True)
                cmd.execute(emulate_dot_seven=True)
