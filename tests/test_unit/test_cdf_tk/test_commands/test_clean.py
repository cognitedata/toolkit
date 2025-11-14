from cognite_toolkit._cdf_tk.commands import CleanCommand
from cognite_toolkit._cdf_tk.data_classes._config_yaml import BuildEnvironment


class TestCleanCommandSelectModules:
    """Test the CleanCommand._select_modules method."""

    def test_select_specific_module(
        self,
        build_environment: BuildEnvironment,
    ) -> None:
        """Test that when a specific module is specified, it is returned."""
        cmd = CleanCommand(silent=True, skip_tracking=True)
        result = cmd._select_modules(build_environment, module_str="my_example_module")
        assert len(result) == 1
        assert result[0].dir.name == "my_example_module", f"Expected 'my_example_module', got {result[0].dir.name}"
