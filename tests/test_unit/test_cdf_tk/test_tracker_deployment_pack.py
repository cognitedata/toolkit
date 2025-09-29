"""Unit tests for deployment pack tracking functionality in Tracker."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from cognite_toolkit._cdf_tk.data_classes import (
    BuildVariables,
    BuiltModule,
    BuiltResource,
    BuiltResourceList,
    SourceLocationEager,
)
from cognite_toolkit._cdf_tk.data_classes._module_directories import ModuleLocation
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

    @pytest.fixture
    def sample_package_with_id(self) -> Package:
        """Create a sample package with an ID for testing."""
        module_location = ModuleLocation(
            dir=Path("/test/test_module"),
            source_absolute_path=Path("/test"),
            source_paths=[Path("/test/test_module/file1.yaml")],
            is_selected=True,
        )
        return Package(
            name="test_package",
            title="Test Package",
            description="A test package",
            id="test-package-123",
            modules=[module_location],
        )

    @pytest.fixture
    def sample_package_without_id(self) -> Package:
        """Create a sample package without an ID for testing."""
        module_location = ModuleLocation(
            dir=Path("/test/test_module_no_id"),
            source_absolute_path=Path("/test"),
            source_paths=[Path("/test/test_module_no_id/file1.yaml")],
            is_selected=True,
        )
        return Package(
            name="test_package_no_id",
            title="Test Package No ID",
            description="A test package without ID",
            id=None,
            modules=[module_location],
        )

    @pytest.fixture
    def sample_built_module(self) -> BuiltModule:
        """Create a sample built module for testing."""
        return BuiltModule(
            name="test_module",
            location=SourceLocationEager(
                path=Path("test/test_module"),
                _hash="test_hash",
            ),
            build_variables=BuildVariables([]),
            resources={
                "files": BuiltResourceList(
                    [
                        BuiltResource("file1", SourceLocationEager(Path("files/file1.yaml"), "hash"), "File", None, []),
                    ]
                ),
            },
            warning_count=0,
            status="success",
            iteration=1,
        )

    def test_track_deployment_pack_download_with_id(self, mock_tracker: Tracker) -> None:
        """Test tracking deployment pack download with package ID."""
        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_deployment_pack_download(
                package_id="test-package-123", package_name="test_package", url="https://example.com/package.zip"
            )

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "deploymentPackDownload"
            assert event_info["package_id"] == "test-package-123"
            assert event_info["package_name"] == "test_package"
            assert event_info["source_url"] == "https://example.com/package.zip"
            assert "toolkitVersion" in event_info
            assert "$os" in event_info

    def test_track_deployment_pack_download_without_id(self, mock_tracker: Tracker) -> None:
        """Test tracking deployment pack download without package ID."""
        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_deployment_pack_download(
                package_id=None, package_name="test_package", url="https://example.com/package.zip"
            )

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "deploymentPackDownload"
            assert "package_id" not in event_info
            assert event_info["package_name"] == "test_package"

    def test_track_deployment_pack_install_init_command(
        self, mock_tracker: Tracker, sample_package_with_id: Package, sample_package_without_id: Package
    ) -> None:
        """Test tracking deployment pack installation via init command."""
        packages = [sample_package_with_id, sample_package_without_id]

        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_deployment_pack_install(packages, "init")

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "deploymentPackInstall"
            assert event_info["command_type"] == "init"
            assert event_info["package_count"] == 2
            assert event_info["package_names"] == ["test_package", "test_package_no_id"]
            assert event_info["package_ids"] == ["test-package-123"]  # Only packages with IDs

    def test_track_deployment_pack_install_add_command(
        self, mock_tracker: Tracker, sample_package_with_id: Package
    ) -> None:
        """Test tracking deployment pack installation via add command."""
        packages = [sample_package_with_id]

        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_deployment_pack_install(packages, "add")

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "deploymentPackInstall"
            assert event_info["command_type"] == "add"
            assert event_info["package_count"] == 1

    def test_track_module_build_with_package_info(
        self, mock_tracker: Tracker, sample_built_module: BuiltModule
    ) -> None:
        """Test tracking module build with package information."""
        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_module_build(
                sample_built_module, package_id="test-package-123", module_id="test-module-456"
            )

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "moduleBuild"
            assert event_info["package_id"] == "test-package-123"
            assert event_info["module_id"] == "test-module-456"
            assert event_info["module"] == "test_module"

    def test_track_module_build_without_package_info(
        self, mock_tracker: Tracker, sample_built_module: BuiltModule
    ) -> None:
        """Test tracking module build without package information (backward compatibility)."""
        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_module_build(sample_built_module)

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "moduleBuild"
            assert "package_id" not in event_info
            assert "module_id" not in event_info
            assert event_info["module"] == "test_module"

    def test_track_module_build_with_pack_id_multiple_modules(
        self, mock_tracker: Tracker, sample_package_with_id: Package
    ) -> None:
        """Test tracking multiple module builds with package information."""
        modules = [
            BuiltModule(
                name="test_module",
                location=SourceLocationEager(Path("test/test_module"), "hash1"),
                build_variables=BuildVariables([]),
                resources={},
                warning_count=0,
                status="success",
                iteration=1,
            ),
            BuiltModule(
                name="other_module",
                location=SourceLocationEager(Path("test/other_module"), "hash2"),
                build_variables=BuildVariables([]),
                resources={},
                warning_count=1,
                status="success",
                iteration=1,
            ),
        ]

        with patch.object(mock_tracker, "track_module_build", return_value=True) as mock_track_build:
            result = mock_tracker.track_module_build_with_pack_id(modules, [sample_package_with_id])

            assert result is True
            assert mock_track_build.call_count == 2

            # First call should have package info
            first_call = mock_track_build.call_args_list[0]
            assert first_call[1]["package_id"] == "test-package-123"

            # Second call should not have package info (module not in package)
            second_call = mock_track_build.call_args_list[1]
            assert second_call[1]["package_id"] is None

    def test_track_module_deploy_with_pack_id(self, mock_tracker: Tracker, sample_package_with_id: Package) -> None:
        """Test tracking module deployment with package information."""
        modules = ["test_module", "other_module"]

        with patch.object(mock_tracker, "_track", return_value=True) as mock_track:
            result = mock_tracker.track_module_deploy_with_pack_id(modules, [sample_package_with_id], dry_run=True)

            assert result is True
            mock_track.assert_called_once()
            args, _ = mock_track.call_args
            event_name, event_info = args

            assert event_name == "moduleDeployWithPackId"
            assert event_info["module_count"] == 2
            assert event_info["modules"] == modules
            assert event_info["dry_run"] is True
            assert event_info["package_ids"] == ["test-package-123"]
            assert event_info["package_names"] == ["test_package"]

    def test_track_module_deploy_empty_modules(self, mock_tracker: Tracker) -> None:
        """Test tracking module deployment with empty module list."""
        result = mock_tracker.track_module_deploy_with_pack_id([], None)
        assert result is False

    def test_track_module_build_empty_modules(self, mock_tracker: Tracker) -> None:
        """Test tracking module build with empty module list."""
        result = mock_tracker.track_module_build_with_pack_id([], None)
        assert result is False

    def test_opt_out_behavior(self, sample_built_module: BuiltModule) -> None:
        """Test that tracking respects opt-out behavior."""
        with patch("cognite_toolkit._cdf_tk.tracker.Mixpanel"):
            tracker = Tracker(skip_tracking=False)
            tracker._opt_status = "opted-out"

            result = tracker.track_deployment_pack_download("test-id", "test-package")
            assert result is False

    def test_skip_tracking_behavior(self, sample_built_module: BuiltModule) -> None:
        """Test that tracking respects skip_tracking flag."""
        with patch("cognite_toolkit._cdf_tk.tracker.Mixpanel"):
            tracker = Tracker(skip_tracking=True)
            tracker._opt_status = "opted-in"

            result = tracker.track_deployment_pack_download("test-id", "test-package")
            assert result is False
