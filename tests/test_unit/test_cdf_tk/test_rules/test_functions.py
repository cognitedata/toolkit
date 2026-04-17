from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionLimits, ResourceLimit
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError
from cognite_toolkit._cdf_tk.rules._functions import FunctionLimitsRule


class TestFunctionLimitsRule:
    """Test suite for FunctionLimitsRule validation."""

    @pytest.fixture
    def function_limits(self) -> FunctionLimits:
        """Create a sample FunctionLimits object for testing."""
        return FunctionLimits(
            timeout_minutes=10,
            cpu_cores=ResourceLimit(min=0.1, max=2.0, default=0.5),
            memory_gb=ResourceLimit(min=0.25, max=4.0, default=1.0),
            runtimes=["py39", "py310", "py311", "py312"],
            response_size_mb=50,
        )

    @staticmethod
    def _write_function_yaml(filepath: Path, content: dict) -> None:
        """Helper to write function YAML content."""
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "w") as f:
            yaml.safe_dump(content, f)

    @staticmethod
    def _create_rule_with_client(function_limits: FunctionLimits) -> FunctionLimitsRule:
        """Create a FunctionLimitsRule with mocked client."""
        mock_client = MagicMock()
        mock_client.tool.functions.limits.return_value = function_limits
        rule = FunctionLimitsRule(modules=[], client=mock_client)
        return rule

    def test_get_status_with_client(self, function_limits: FunctionLimits) -> None:
        """Test get_status returns ready when client is available."""
        rule = self._create_rule_with_client(function_limits)
        status = rule.get_status()
        assert status.code == "ready"
        assert "validate function definitions" in status.message.lower()

    def test_get_status_without_client(self) -> None:
        """Test get_status returns unavailable when no client is provided."""
        rule = FunctionLimitsRule(modules=[])
        status = rule.get_status()
        assert status.code == "unavailable"
        assert "client" in status.message.lower()

    def test_validate_function_cpu_exceeds_max(self, tmp_path: Path, function_limits: FunctionLimits) -> None:
        """Test validation error when CPU cores exceed maximum limit."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(
            yaml_file,
            {
                "externalId": "my_function",
                "name": "My Function",
                "cpu": 3.0,
            },
        )
        rule = self._create_rule_with_client(function_limits)
        errors = list(rule._validate_function(yaml_file))
        assert len(errors) == 1
        assert isinstance(errors[0], ConsistencyError)
        assert "CPU cores" in errors[0].message
        assert "3.0" in errors[0].message

    def test_validate_function_cpu_below_min(self, tmp_path: Path, function_limits: FunctionLimits) -> None:
        """Test validation error when CPU cores are below minimum limit."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(
            yaml_file,
            {
                "externalId": "my_function",
                "name": "My Function",
                "cpu": 0.05,
            },
        )
        rule = self._create_rule_with_client(function_limits)
        errors = list(rule._validate_function(yaml_file))
        assert len(errors) == 1
        assert "CPU cores" in errors[0].message

    def test_validate_function_memory_exceeds_max(self, tmp_path: Path, function_limits: FunctionLimits) -> None:
        """Test validation error when memory exceeds maximum limit."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(
            yaml_file,
            {
                "externalId": "my_function",
                "name": "My Function",
                "memory": 5.0,
            },
        )
        rule = self._create_rule_with_client(function_limits)
        errors = list(rule._validate_function(yaml_file))
        assert len(errors) == 1
        assert "memory" in errors[0].message

    def test_validate_function_memory_below_min(self, tmp_path: Path, function_limits: FunctionLimits) -> None:
        """Test validation error when memory is below minimum limit."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(
            yaml_file,
            {
                "externalId": "my_function",
                "name": "My Function",
                "memory": 0.1,
            },
        )
        rule = self._create_rule_with_client(function_limits)
        errors = list(rule._validate_function(yaml_file))
        assert len(errors) == 1

    def test_validate_function_valid_resources(self, tmp_path: Path, function_limits: FunctionLimits) -> None:
        """Test no errors when function resources are within limits."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(
            yaml_file,
            {
                "externalId": "my_function",
                "name": "My Function",
                "cpu": 1.0,
                "memory": 2.0,
            },
        )
        rule = self._create_rule_with_client(function_limits)
        errors = list(rule._validate_function(yaml_file))
        assert len(errors) == 0

    def test_validate_function_none_resources(self, tmp_path: Path, function_limits: FunctionLimits) -> None:
        """Test no errors when resources are not specified."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(
            yaml_file,
            {
                "externalId": "my_function",
                "name": "My Function",
            },
        )
        rule = self._create_rule_with_client(function_limits)
        errors = list(rule._validate_function(yaml_file))
        assert len(errors) == 0

    def test_validate_function_multiple_violations(self, tmp_path: Path, function_limits: FunctionLimits) -> None:
        """Test reporting multiple validation errors."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(
            yaml_file,
            {
                "externalId": "my_function",
                "name": "My Function",
                "cpu": 3.0,
                "memory": 5.0,
            },
        )
        rule = self._create_rule_with_client(function_limits)
        errors = list(rule._validate_function(yaml_file))
        assert len(errors) == 2

    def test_validate_function_consistency_error_attributes(
        self, tmp_path: Path, function_limits: FunctionLimits
    ) -> None:
        """Test that ConsistencyError has required attributes."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(
            yaml_file,
            {
                "externalId": "my_function",
                "name": "My Function",
                "cpu": 5.0,
            },
        )
        rule = self._create_rule_with_client(function_limits)
        errors = list(rule._validate_function(yaml_file))
        assert len(errors) == 1
        error = errors[0]
        assert error.code == "FUNCTION-CPU"
        assert error.message is not None
        assert error.fix is not None

    def test_limits_property_raises_without_client(self) -> None:
        """Test that limits property raises when no client is available."""
        rule = FunctionLimitsRule(modules=[])
        with pytest.raises(ValueError, match="Client is required"):
            _ = rule.limits
