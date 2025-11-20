from pathlib import Path

import pytest
import typer

from cognite_toolkit._cdf_tk.apps import DevApp
from cognite_toolkit._cdf_tk.commands import ResourcesCommand


class TestResourcesCommand:
    def test_create_success(self, tmp_path: Path) -> None:
        """Test create method executes successfully without errors."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        organization_dir.mkdir(parents=True, exist_ok=True)
        cmd.create(
            organization_dir=organization_dir,
            module_name="test_module",
            resource_directory="data_models",
            resources=["space", "container"],
            file_name=["my_space", "my_container"],
            verbose=False,
        )
        assert cmd is not None

    def test_create_bad_parameter_mismatched_file_names(self, tmp_path: Path) -> None:
        """Test that BadParameter error is raised when number of file names doesn't match number of resources."""
        organization_dir = tmp_path / "my_org"
        organization_dir.mkdir(parents=True, exist_ok=True)
        app = DevApp()
        with pytest.raises(typer.BadParameter, match="Number of resources must match number of file names"):
            app.create(
                module="test_module",
                resource_directory="data_models",
                resources=["space", "container"],
                file_names=["my_space"],
                verbose=False,
                organization_dir=organization_dir,
            )

    def test_create_interactive_with_variations(self, tmp_path: Path) -> None:
        """Test create_interactive method with various parameter combinations."""
        cmd = ResourcesCommand(print_warning=False, skip_tracking=True, silent=True)
        organization_dir = tmp_path / "my_org"
        organization_dir.mkdir(parents=True, exist_ok=True)

        # Test with empty resource_directories
        cmd.create_interactive(
            organization_dir=organization_dir,
            module_name="test_module",
            resource_directories=[],
            verbose=True,
        )
        assert cmd is not None

        # Test with multiple resource_directories
        cmd.create_interactive(
            organization_dir=organization_dir,
            module_name="another_module",
            resource_directories=["data_models", "functions", "workflows"],
            verbose=False,
        )
        assert cmd is not None
