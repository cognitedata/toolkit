"""Unit tests for modules command tracking functionality."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._cdf_tk.data_classes._packages import Package, Packages


class TestModulesCommandTracking:
    """Test tracking functionality in ModulesCommand."""

    @pytest.fixture
    def mock_modules_command(self) -> ModulesCommand:
        """Create a ModulesCommand with mocked tracker."""
        command = ModulesCommand(skip_tracking=False)
        command.tracker = MagicMock()
        return command

    def test_download_tracking(self, mock_modules_command: ModulesCommand) -> None:
        """Test that external library downloads are tracked."""
        # Create mock packages
        package = Package(name="test_package", title="Test Package", id="test-123")
        packages = Packages({"test_package": package})

        # Mock the download and loading process
        with (
            patch("cognite_toolkit._cdf_tk.commands.modules.CDFToml") as mock_cdf_toml,
            patch("cognite_toolkit._cdf_tk.commands.modules.Flags") as mock_flags,
            patch.object(mock_modules_command, "_download"),
            patch.object(mock_modules_command, "_validate_checksum"),
            patch.object(mock_modules_command, "_unpack"),
            patch.object(mock_modules_command, "_validate_packages"),
            patch("cognite_toolkit._cdf_tk.commands.modules.Packages") as mock_packages_class,
        ):
            # Setup mocks
            mock_flags.EXTERNAL_LIBRARIES.is_enabled.return_value = True
            mock_library = MagicMock()
            mock_library.url = "https://example.com/package.zip"
            mock_library.checksum = "test-checksum"
            mock_cdf_toml.load.return_value.libraries = {"test_lib": mock_library}
            mock_packages_class.return_value.load.return_value = packages

            # Call the method
            _, _ = mock_modules_command._get_available_packages()

            # Verify tracking was called
            mock_modules_command.tracker.track_deployment_pack_download.assert_called_once_with(
                package_id="test-123", package_name="test_package", url="https://example.com/package.zip"
            )

    def test_init_tracking_fix(self, mock_modules_command: ModulesCommand) -> None:
        """Test that init command uses self.tracker instead of creating new Tracker."""
        packages = [Package(name="test_package", title="Test Package", id="test-123")]
        selected_packages = Packages({"test": packages[0]})

        with (
            patch.object(mock_modules_command, "_get_available_packages") as mock_get_packages,
            patch.object(mock_modules_command, "_verify_clean") as mock_verify_clean,
            patch.object(mock_modules_command, "_select_packages") as mock_select_packages,
            patch.object(mock_modules_command, "_get_download_data") as mock_get_download_data,
            patch.object(mock_modules_command, "_create"),
            patch("cognite_toolkit._cdf_tk.commands.modules.Panel"),
            patch("cognite_toolkit._cdf_tk.commands.modules.print"),
        ):
            # Setup mocks
            mock_get_packages.return_value = (selected_packages, Path("/test"))
            mock_verify_clean.return_value = "new"
            mock_select_packages.return_value = selected_packages
            mock_get_download_data.return_value = False

            # Call init with select_all=True to avoid interactive flow and typer.Exit
            mock_modules_command.init(
                organization_dir=Path("."),
                select_all=True,
                clean=False,
                user_environments=["dev"],
                user_download_data=False,
            )

            # Verify tracking was called with self.tracker
            mock_modules_command.tracker.track_deployment_pack_install.assert_called_once_with(
                list(selected_packages.values()), command_type="init"
            )

    def test_add_tracking(self, mock_modules_command: ModulesCommand) -> None:
        """Test that add command tracks package installation."""
        packages = [Package(name="test_package", title="Test Package", id="test-123")]

        with (
            patch("cognite_toolkit._cdf_tk.commands.modules.verify_module_directory"),
            patch("cognite_toolkit._cdf_tk.commands.modules.ModuleResources") as mock_module_resources,
            patch("cognite_toolkit._cdf_tk.commands.modules.BuildConfigYAML"),
            patch.object(mock_modules_command, "_get_available_packages") as mock_get_packages,
            patch.object(mock_modules_command, "_select_packages") as mock_select_packages,
            patch.object(mock_modules_command, "_get_download_data") as mock_get_download_data,
            patch.object(mock_modules_command, "_create"),
        ):
            # Setup mocks
            mock_module_resources.return_value.list.return_value = []
            mock_get_packages.return_value = (Packages({"test": packages[0]}), Path("/test"))
            mock_select_packages.return_value = Packages({"test": packages[0]})
            mock_get_download_data.return_value = False

            # Mock config file exists
            org_dir = Path("/test/org")
            with patch.object(Path, "exists", return_value=True):
                # Call add
                mock_modules_command.add(org_dir)

            # Verify tracking was called
            mock_modules_command.tracker.track_deployment_pack_install.assert_called_once_with(
                packages, command_type="add"
            )
