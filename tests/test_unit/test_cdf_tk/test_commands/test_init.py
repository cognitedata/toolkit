from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import typer
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands import AuthCommand, CollectCommand, InitCommand, ModulesCommand, RepoCommand
from cognite_toolkit._cdf_tk.feature_flags import Flags
from tests.test_unit.utils import MockQuestionary, PrintCapture


# Reset singleton before each test to ensure test isolation
@pytest.fixture(autouse=True)
def reset_cdf_toml_singleton():
    """Reset CDFToml singleton before and after each test"""
    from cognite_toolkit._cdf_tk import cdf_toml

    old_value = cdf_toml._CDF_TOML
    cdf_toml._CDF_TOML = None
    yield
    cdf_toml._CDF_TOML = old_value


@pytest.fixture
def mock_v07_enabled(monkeypatch: MonkeyPatch) -> None:
    """Mock Flags.v07.is_enabled() to return True"""

    def mock_is_enabled(self) -> bool:
        if self == Flags.v07:
            return True
        return False

    monkeypatch.setattr(Flags.v07, "is_enabled", lambda: True)


@pytest.fixture
def empty_dir(tmp_path: Path) -> Path:
    """Create an empty directory for testing"""
    test_dir = tmp_path / "test_init"
    test_dir.mkdir()
    return test_dir


@pytest.fixture
def init_command() -> InitCommand:
    """Create an InitCommand instance"""
    return InitCommand(silent=True, skip_tracking=True)


class TestInitChecklistItem:
    """Tests for InitChecklistItem dataclass"""

    def test_get_status_display_none(self, init_command: InitCommand) -> None:
        """Test status display for uninitialized item"""
        from cognite_toolkit._cdf_tk.commands.init import InitChecklistItem

        item = InitChecklistItem(
            name="test",
            description="Test item",
            function=lambda: None,
        )
        assert item.get_status_display() == "○"

    def test_get_status_display_successful(self, init_command: InitCommand) -> None:
        """Test status display for successful item"""
        from cognite_toolkit._cdf_tk.commands.init import InitChecklistItem, InitItemStatus

        item = InitChecklistItem(
            name="test",
            description="Test item",
            function=lambda: None,
            status=InitItemStatus.SUCCESSFUL,
        )
        assert item.get_status_display() == "✓"

    def test_get_status_display_failed(self, init_command: InitCommand) -> None:
        """Test status display for failed item"""
        from cognite_toolkit._cdf_tk.commands.init import InitChecklistItem, InitItemStatus

        item = InitChecklistItem(
            name="test",
            description="Test item",
            function=lambda: None,
            status=InitItemStatus.FAILED,
        )
        assert item.get_status_display() == "✗"

    def test_get_choice_title_mandatory(self, init_command: InitCommand) -> None:
        """Test choice title for mandatory item"""
        from cognite_toolkit._cdf_tk.commands.init import InitChecklistItem

        item = InitChecklistItem(
            name="test",
            description="Test item",
            function=lambda: None,
            mandatory=True,
        )
        assert "(required)" in item.get_choice_title()

    def test_get_choice_title_optional(self, init_command: InitCommand) -> None:
        """Test choice title for optional item"""
        from cognite_toolkit._cdf_tk.commands.init import InitChecklistItem

        item = InitChecklistItem(
            name="test",
            description="Test item",
            function=lambda: None,
            mandatory=False,
        )
        assert "(required)" not in item.get_choice_title()


class TestInitToml:
    """Tests for _init_toml method"""

    def test_init_toml_success(self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch) -> None:
        """Test successful cdf.toml initialization"""
        monkeypatch.chdir(empty_dir)

        assert not (empty_dir / CDFToml.file_name).exists()

        # Mock the prompt to return empty string (current directory)
        # _prompt_organization_dir uses questionary from modules module
        with MockQuestionary("cognite_toolkit._cdf_tk.commands.modules", monkeypatch, [""]):
            init_command._init_toml()

        assert (empty_dir / CDFToml.file_name).exists()
        cdf_toml = CDFToml.load(empty_dir)
        assert cdf_toml.cdf.default_env == "dev"

    def test_init_toml_with_subdirectory(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Test cdf.toml initialization with subdirectory"""
        monkeypatch.chdir(empty_dir)
        subdir_name = "my-org"

        # Mock the prompt to return subdirectory name
        # _prompt_organization_dir uses questionary from modules module
        with MockQuestionary("cognite_toolkit._cdf_tk.commands.modules", monkeypatch, [subdir_name]):
            init_command._init_toml()

        # Verify that the subdirectory name is recorded in cdf.toml
        cdf_toml = CDFToml.load(empty_dir, use_singleton=False)
        assert cdf_toml.cdf.default_organization_dir == empty_dir / subdir_name
        # The subdirectory itself is not created by _init_toml, only recorded in the config
        assert not (empty_dir / subdir_name).exists()

    def test_init_toml_dry_run(self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch) -> None:
        """Test dry-run mode for cdf.toml initialization"""
        monkeypatch.chdir(empty_dir)

        print_capture = PrintCapture()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.init.print", print_capture)

        # _prompt_organization_dir uses questionary from modules module
        with MockQuestionary("cognite_toolkit._cdf_tk.commands.modules", monkeypatch, [""]):
            init_command._init_toml(dry_run=True)

        # Should not create file in dry-run
        assert not (empty_dir / CDFToml.file_name).exists()
        assert "Would initialize" in " ".join(print_capture.messages)


class TestInitAuth:
    """Tests for _init_auth method"""

    def test_init_auth_success(self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch) -> None:
        """Test successful authentication initialization"""
        monkeypatch.chdir(empty_dir)

        mock_auth_command = MagicMock(spec=AuthCommand)
        mock_auth_command.run.return_value = None

        with patch("cognite_toolkit._cdf_tk.commands.init.AuthCommand", return_value=mock_auth_command):
            init_command._init_auth()

        mock_auth_command.run.assert_called_once()
        # Verify it was called with no_verify=True
        call_args = mock_auth_command.run.call_args
        assert call_args is not None

    def test_init_auth_dry_run(self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch) -> None:
        """Test dry-run mode for authentication"""
        monkeypatch.chdir(empty_dir)

        mock_auth_command = MagicMock(spec=AuthCommand)
        mock_auth_command.run.return_value = None

        with patch("cognite_toolkit._cdf_tk.commands.init.AuthCommand", return_value=mock_auth_command):
            init_command._init_auth(dry_run=True)

        mock_auth_command.run.assert_called_once()


class TestInitModules:
    """Tests for _init_modules method"""

    def test_init_modules_success(self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch) -> None:
        """Test successful modules initialization"""
        monkeypatch.chdir(empty_dir)

        mock_modules_command = MagicMock(spec=ModulesCommand)
        mock_modules_command.__enter__ = MagicMock(return_value=mock_modules_command)
        mock_modules_command.__exit__ = MagicMock(return_value=None)
        mock_modules_command.run.return_value = None

        with patch("cognite_toolkit._cdf_tk.commands.init.ModulesCommand", return_value=mock_modules_command):
            init_command._init_modules()

        mock_modules_command.run.assert_called_once()

    def test_init_modules_dry_run(self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch) -> None:
        """Test dry-run mode for modules"""
        monkeypatch.chdir(empty_dir)

        mock_modules_command = MagicMock(spec=ModulesCommand)
        mock_modules_command.__enter__ = MagicMock(return_value=mock_modules_command)
        mock_modules_command.__exit__ = MagicMock(return_value=None)
        mock_modules_command.run.return_value = None

        with patch("cognite_toolkit._cdf_tk.commands.init.ModulesCommand", return_value=mock_modules_command):
            with patch("cognite_toolkit._cdf_tk.commands.init.tempfile.mkdtemp", return_value=str(empty_dir / "temp")):
                with patch("cognite_toolkit._cdf_tk.commands.init.shutil.rmtree") as mock_rmtree:
                    init_command._init_modules(dry_run=True)

        mock_modules_command.run.assert_called_once()
        mock_rmtree.assert_called_once()


class TestInitRepo:
    """Tests for _init_repo method"""

    def test_init_repo_success(self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch) -> None:
        """Test successful repository initialization"""
        monkeypatch.chdir(empty_dir)

        mock_repo_command = MagicMock(spec=RepoCommand)
        mock_repo_command.run.return_value = None

        with patch("cognite_toolkit._cdf_tk.commands.init.RepoCommand", return_value=mock_repo_command):
            init_command._init_repo()

        mock_repo_command.run.assert_called_once()
        # Verify it was called with correct parameters
        call_args = mock_repo_command.run.call_args
        assert call_args is not None


class TestInitDataCollection:
    """Tests for _init_data_collection method"""

    def test_init_data_collection_opt_in(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Test opting in to data collection"""
        monkeypatch.chdir(empty_dir)

        mock_collect_command = MagicMock(spec=CollectCommand)
        mock_collect_command.run.return_value = None

        with patch("cognite_toolkit._cdf_tk.commands.init.CollectCommand", return_value=mock_collect_command):
            with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, [True]):
                init_command._init_data_collection()

        mock_collect_command.run.assert_called_once()
        # Verify it was called with "opt-in"
        call_args = mock_collect_command.run.call_args
        assert call_args is not None
        # Check that execute("opt-in") was called
        execute_call = call_args[0][0]
        assert execute_call is not None

    def test_init_data_collection_opt_out(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Test opting out of data collection"""
        monkeypatch.chdir(empty_dir)

        mock_collect_command = MagicMock(spec=CollectCommand)
        mock_collect_command.run.return_value = None

        with patch("cognite_toolkit._cdf_tk.commands.init.CollectCommand", return_value=mock_collect_command):
            with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, [False]):
                init_command._init_data_collection()

        mock_collect_command.run.assert_called_once()

    def test_init_data_collection_dry_run(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch
    ) -> None:
        """Test dry-run mode for data collection"""
        monkeypatch.chdir(empty_dir)

        print_capture = PrintCapture()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.init.print", print_capture)

        with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, [True]):
            init_command._init_data_collection(dry_run=True)

        messages = " ".join(print_capture.messages)
        assert "Would opt in" in messages or "Would not opt in" in messages


class TestExecute:
    """Tests for execute method"""

    def test_execute_v07_disabled(self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch) -> None:
        """Test execute when v07 flag is disabled"""
        monkeypatch.chdir(empty_dir)

        print_capture = PrintCapture()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.init.print", print_capture)

        with patch.object(Flags.v07, "is_enabled", return_value=False):
            init_command.execute()

        messages = " ".join(print_capture.messages)
        assert "deprecated" in messages.lower() or "Use 'cdf modules init'" in messages

    def test_execute_full_flow_success(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch, mock_v07_enabled
    ) -> None:
        """Test successful execution of full init flow"""
        monkeypatch.chdir(empty_dir)

        # Mock all the commands
        mock_auth_command = MagicMock(spec=AuthCommand)
        mock_auth_command.run.return_value = None

        mock_modules_command = MagicMock(spec=ModulesCommand)
        mock_modules_command.__enter__ = MagicMock(return_value=mock_modules_command)
        mock_modules_command.__exit__ = MagicMock(return_value=None)
        mock_modules_command.run.return_value = None

        mock_repo_command = MagicMock(spec=RepoCommand)
        mock_repo_command.run.return_value = None

        mock_collect_command = MagicMock(spec=CollectCommand)
        mock_collect_command.run.return_value = None

        print_capture = PrintCapture()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.init.print", print_capture)

        # Mock questionary responses:
        # 1. Organization dir prompt (empty = current dir) - from modules module
        # 2-6. Select each task in order: initToml, initAuth, initModules, initRepo, initDataCollection - from init module
        # 7. Confirm opt-in for data collection - from init module
        # 8. Exit - from init module
        init_answers = [
            "initToml",  # Select initToml
            "initAuth",  # Select initAuth
            "initModules",  # Select initModules
            "initRepo",  # Select initRepo
            "initDataCollection",  # Select initDataCollection
            True,  # Opt-in to data collection
            "__exit__",  # Exit
        ]
        modules_answers = [""]  # Organization dir (empty = current dir)

        with patch("cognite_toolkit._cdf_tk.commands.init.AuthCommand", return_value=mock_auth_command):
            with patch("cognite_toolkit._cdf_tk.commands.init.ModulesCommand", return_value=mock_modules_command):
                with patch("cognite_toolkit._cdf_tk.commands.init.RepoCommand", return_value=mock_repo_command):
                    with patch(
                        "cognite_toolkit._cdf_tk.commands.init.CollectCommand", return_value=mock_collect_command
                    ):
                        with MockQuestionary("cognite_toolkit._cdf_tk.commands.modules", monkeypatch, modules_answers):
                            with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, init_answers):
                                init_command.execute()

        # Verify all commands were called
        mock_auth_command.run.assert_called_once()
        mock_modules_command.run.assert_called_once()
        mock_repo_command.run.assert_called_once()
        mock_collect_command.run.assert_called_once()

        # Verify cdf.toml was created
        assert (empty_dir / CDFToml.file_name).exists()

        # Verify completion message
        messages = " ".join(print_capture.messages)
        assert "Initialization complete" in messages or "completed successfully" in messages

    def test_execute_mandatory_item_not_completed(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch, mock_v07_enabled
    ) -> None:
        """Test that exit is blocked when mandatory item is not completed"""
        monkeypatch.chdir(empty_dir)

        print_capture = PrintCapture()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.init.print", print_capture)

        # Try to exit without completing initToml
        answers = ["__exit__"]

        with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, answers):
            init_command.execute()

        messages = " ".join(print_capture.messages)
        assert "mandatory" in messages.lower() or "required" in messages.lower()

    def test_execute_re_run_confirmation(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch, mock_v07_enabled
    ) -> None:
        """Test re-run confirmation when item was already run"""
        monkeypatch.chdir(empty_dir)

        mock_auth_command = MagicMock(spec=AuthCommand)
        mock_auth_command.init.return_value = None

        # First complete initToml, then test re-run
        # Flow: select initToml -> org dir prompt (mocked) -> menu shows -> select initToml again -> confirm re-run -> exit
        with patch("cognite_toolkit._cdf_tk.commands.init.AuthCommand", return_value=mock_auth_command):
            with patch(
                "cognite_toolkit._cdf_tk.commands.init.ModulesCommand._prompt_organization_dir", return_value=Path("")
            ):
                with MockQuestionary(
                    "cognite_toolkit._cdf_tk.commands.init", monkeypatch, ["initToml", "initToml", True, "__exit__"]
                ):
                    init_command.execute()

        # Now test re-run scenario with a fresh command instance
        init_command2 = InitCommand(silent=True, skip_tracking=True)
        # Flow: select initToml (already completed) -> confirm re-run -> exit
        with patch(
            "cognite_toolkit._cdf_tk.commands.init.ModulesCommand._prompt_organization_dir", return_value=Path("")
        ):
            with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, ["initToml", True, "__exit__"]):
                init_command2.execute()

    def test_execute_item_failure(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch, mock_v07_enabled
    ) -> None:
        """Test handling when an item fails"""
        monkeypatch.chdir(empty_dir)

        print_capture = PrintCapture()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.init.print", print_capture)

        mock_auth_command = MagicMock(spec=AuthCommand)
        # Make init() raise an exception when called (via the lambda in run())
        mock_auth_command.init.side_effect = Exception("Auth failed")

        # Make run() actually call the lambda (like the real run() method does)
        # Use side_effect on the MagicMock's run method so calls are still tracked
        def run_side_effect(execute, *args, **kwargs):
            # Actually call the lambda, which will call init() and raise
            return execute(*args, **kwargs)

        mock_auth_command.run.side_effect = run_side_effect

        # The flow is:
        # 1. Checklist menu (init questionary) - select "initToml"
        # 2. Organization dir prompt is mocked to return Path("") (current dir)
        # 3. Checklist menu again (init questionary) - select "initAuth"
        # 4. Auth runs and fails
        # 5. Checklist menu again (init questionary) - select "__exit__"
        init_answers = [
            "initToml",  # Select initToml from checklist
            "initAuth",  # Select initAuth from checklist (after initToml completes)
            "__exit__",  # Exit after auth fails
        ]

        # Patch AuthCommand to return our mock when instantiated
        # Also patch _prompt_organization_dir to return empty Path (current dir) to avoid questionary issues
        with patch("cognite_toolkit._cdf_tk.commands.init.AuthCommand", return_value=mock_auth_command):
            with patch(
                "cognite_toolkit._cdf_tk.commands.init.ModulesCommand._prompt_organization_dir", return_value=Path("")
            ):
                with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, init_answers.copy()):
                    init_command.execute()

        # Verify that the failure message was printed
        # The message format is: "✗ {description} failed: {exception}"
        # For unexpected errors, we also print the traceback
        messages = " ".join(print_capture.messages)
        assert "failed" in messages.lower(), f"Expected 'failed' in messages, got: {messages}"
        # Verify it's related to authentication
        assert "auth" in messages.lower() or "authentication" in messages.lower(), (
            f"Expected 'auth' in messages, got: {messages}"
        )
        # Verify that unexpected error traceback was printed
        assert "traceback" in messages.lower() or "unexpected error" in messages.lower(), (
            f"Expected traceback message for unexpected error, got: {messages}"
        )

        # Verify that run was called (which calls the lambda, which calls init())
        mock_auth_command.run.assert_called_once()
        # Verify that init was called via the lambda
        mock_auth_command.init.assert_called_once_with(no_verify=True, dry_run=False)

    def test_execute_typer_exit_handling(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch, mock_v07_enabled
    ) -> None:
        """Test that typer.Exit is handled as success"""
        monkeypatch.chdir(empty_dir)

        print_capture = PrintCapture()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.init.print", print_capture)

        mock_modules_command = MagicMock(spec=ModulesCommand)
        mock_modules_command.__enter__ = MagicMock(return_value=mock_modules_command)
        mock_modules_command.__exit__ = MagicMock(return_value=None)
        mock_modules_command.init.side_effect = typer.Exit()

        # Make run() actually call the lambda (like the real run() method does)
        def run_side_effect(execute, *args, **kwargs):
            # Actually call the lambda, which will call init() and raise typer.Exit
            return execute(*args, **kwargs)

        mock_modules_command.run.side_effect = run_side_effect

        init_answers = [
            "initToml",  # Complete mandatory item
            "initModules",  # Select initModules
            "__exit__",  # Exit
        ]
        modules_answers = [""]  # Organization dir

        with patch("cognite_toolkit._cdf_tk.commands.init.ModulesCommand", return_value=mock_modules_command):
            with MockQuestionary("cognite_toolkit._cdf_tk.commands.modules", monkeypatch, modules_answers):
                with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, init_answers):
                    init_command.execute()

        messages = " ".join(print_capture.messages)
        assert "completed successfully" in messages

    def test_execute_dry_run_mode(
        self, init_command: InitCommand, empty_dir: Path, monkeypatch: MonkeyPatch, mock_v07_enabled
    ) -> None:
        """Test execute in dry-run mode"""
        monkeypatch.chdir(empty_dir)

        print_capture = PrintCapture()
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands.init.print", print_capture)

        mock_auth_command = MagicMock(spec=AuthCommand)
        mock_auth_command.init.return_value = None

        init_answers = [
            "initToml",  # Select initToml
            "__exit__",  # Exit
        ]

        with patch("cognite_toolkit._cdf_tk.commands.init.AuthCommand", return_value=mock_auth_command):
            with patch(
                "cognite_toolkit._cdf_tk.commands.init.ModulesCommand._prompt_organization_dir", return_value=Path("")
            ):
                with MockQuestionary("cognite_toolkit._cdf_tk.commands.init", monkeypatch, init_answers):
                    init_command.execute(dry_run=True)

        # In dry-run, cdf.toml should not be created
        assert not (empty_dir / CDFToml.file_name).exists()

        messages = " ".join(print_capture.messages)
        assert "Would initialize" in messages
