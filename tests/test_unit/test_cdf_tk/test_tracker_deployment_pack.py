"""Unit tests for deployment pack tracking functionality in Tracker."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.data_classes._packages import Package
from cognite_toolkit._cdf_tk.tracker import Tracker


class TestTrackerDeploymentPack:
    """Test deployment pack tracking functionality."""

    @pytest.fixture
    def mock_tracker(self) -> Tracker:
        """Create a tracker instance for testing with mocked mixpanel."""
        with patch("cognite_toolkit._cdf_tk.tracker.Mixpanel"):
            tracker = Tracker(skip_tracking=False)
            tracker._opt_status = "opted-in"
            return tracker

    def test_track_deployment_pack_download_with_id(self, mock_tracker: Tracker) -> None:
        """Test tracking deployment pack download with package ID."""
        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_deployment_pack_download(
                package_id="test-package-123",
                package_name="test_package",
                url="https://example.com/package.zip",
            )

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "deploymentPackDownload"
            assert event_info["package_id"] == "test-package-123"
            assert event_info["package_name"] == "test_package"

    def test_track_deployment_pack_install(self, mock_tracker: Tracker) -> None:
        """Test tracking deployment pack installation."""
        package = Package(name="test_package", title="Test Package", id="test-package-123")
        packages = [package]

        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_deployment_pack_install(packages, "init")

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "deploymentPackInstall"
            assert event_info["command_type"] == "init"
            assert event_info["package_count"] == 1

    def test_track_module_build_with_package_info(self, mock_tracker: Tracker) -> None:
        """Test enhanced module build tracking with package information."""
        from cognite_toolkit._cdf_tk.data_classes import BuildVariables, BuiltModule, SourceLocationEager

        module = BuiltModule(
            name="test_module",
            location=SourceLocationEager(Path("test/test_module"), "hash"),
            build_variables=BuildVariables([]),
            resources={},
            warning_count=0,
            status="success",
            iteration=1,
        )

        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_module_build(module, package_id="test-package-123")

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "moduleBuild"
            assert event_info["package_id"] == "test-package-123"
            assert event_info["module"] == "test_module"

    def test_opt_out_behavior(self) -> None:
        """Test that tracking respects opt-out behavior."""
        with patch("cognite_toolkit._cdf_tk.tracker.Mixpanel"):
            tracker = Tracker(skip_tracking=False)
            tracker._opt_status = "opted-out"

            result = tracker.track_deployment_pack_download("test-id", "test-package")
            assert result is False
