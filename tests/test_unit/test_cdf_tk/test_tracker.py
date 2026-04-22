"""Unit tests for deployment pack tracking functionality in Tracker."""

import sys
from unittest.mock import patch

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand, _parse_sys_args


class MockToolkitCommand(ToolkitCommand):
    """Create a toolkit command instance for testing with mocked mixpanel."""

    def __init__(self) -> None:
        super().__init__(print_warning=True, skip_tracking=False, silent=False, client=None)

    def execute(self) -> None:
        pass


class TestTracker:
    """Test command tracking functionality."""

    def test_tracking_command(self) -> None:
        """Ensure command tracking routes through the command's tracker."""

        cmd = MockToolkitCommand()
        with patch.object(cmd.tracker, "_track", return_value=True) as mock_track_internal:
            cmd._additional_tracking_info.downloaded_library_ids.add("test")
            cmd.run(cmd.execute)

            mock_track_internal.assert_called_once()
            *_, event_information = mock_track_internal.call_args.args
            assert "downloadedLibraryIds" in event_information
            assert event_information["downloadedLibraryIds"] == ["test"]


class TestParseSystemArgs:
    """Test _parse_sys_args filtering logic directly."""

    KNOWN = frozenset({"build", "deploy", "clean", "data", "upload", "dir", "download", "purge", "modules", "upgrade"})

    def _parse(self, argv: list[str]) -> list[str]:
        original = sys.argv
        try:
            sys.argv = argv
            return _parse_sys_args(self.KNOWN)
        finally:
            sys.argv = original

    def test_known_subcommands_are_tracked(self) -> None:
        assert self._parse(["cdf", "modules", "upgrade", "--dry-run"]) == ["modules", "upgrade"]

    def test_flag_values_are_excluded(self) -> None:
        result = self._parse(["cdf", "deploy", "--project", "my-secret-project", "--env-file", "/secret/.env"])
        assert result == ["deploy"]
        assert "my-secret-project" not in result
        assert "/secret/.env" not in result

    def test_flag_with_equals_value_is_excluded(self) -> None:
        assert self._parse(["cdf", "build", "--env-file=/secret/.env"]) == ["build"]

    def test_bare_positional_arg_not_a_command_is_excluded(self) -> None:
        """e.g. 'cdf data upload dir secret_name' — secret_name must not be tracked."""
        assert self._parse(["cdf", "data", "upload", "dir", "secret_name"]) == ["data", "upload", "dir"]

    def test_path_positional_arg_is_excluded(self) -> None:
        assert self._parse(["cdf", "build", "path/to/build_dir"]) == ["build"]

    def test_empty_argv(self) -> None:
        assert self._parse(["cdf"]) == []
