"""Unit tests for deployment pack tracking functionality in Tracker."""

from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.tracker import Tracker


class MockToolkitCommand(ToolkitCommand):
    """Create a toolkit command instance for testing with mocked mixpanel."""

    def __init__(self) -> None:
        super().__init__(print_warning=True, skip_tracking=False, silent=False)

    def execute(self) -> None:
        pass


class TestTracker:
    """Test command tracking functionality."""

    @pytest.fixture
    def mock_tracker(self) -> Tracker:
        """Create a tracker instance for testing with mocked mixpanel."""
        with patch("cognite_toolkit._cdf_tk.tracker.Mixpanel"):
            tracker = Tracker(skip_tracking=False)
            tracker._opt_status = "opted-in"
            return tracker

    def test_basic_info(self) -> None:
        """Ensure command tracking routes through the command's tracker."""
        cmd = MockToolkitCommand()

        with patch.object(cmd.tracker, "track_cli_command") as mock_track:
            cmd.run(cmd.execute)

        mock_track.assert_called_once_with([], "success", "MockToolkit", {})

        with patch.object(cmd.tracker, "_track", return_value=True) as mock_track_internal:
            dummy_warnings: list = []
            cmd.tracker.track_cli_command(dummy_warnings, "success", "MockToolkit")

        mock_track_internal.assert_called_once()
        _, event_information = mock_track_internal.call_args.args
        assert "userInput" in event_information

    def test_basic_info_with_additional_tracking_info(self) -> None:
        """Ensure command tracking routes through the command's tracker."""

        cmd = MockToolkitCommand()
        with patch.object(cmd.tracker, "_track", return_value=True) as mock_track_internal:
            cmd._additional_tracking_info = {"additional_tracking_info": ["test"]}
            cmd.run(cmd.execute)

            mock_track_internal.assert_called_once()
            _, event_information = mock_track_internal.call_args.args
            assert "userInput" in event_information
            assert event_information["additional_tracking_info"] == ["test"]
