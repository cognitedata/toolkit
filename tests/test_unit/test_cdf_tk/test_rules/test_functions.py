from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function import (
    FunctionLimits,
    FunctionsActivation,
    ResourceLimit,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, FunctionsAcl
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import BuildError
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._module import ModuleId
from cognite_toolkit._cdf_tk.resource_ios import FunctionIO, ResourceType
from cognite_toolkit._cdf_tk.rules._functions import FunctionRuleSet
from cognite_toolkit._cdf_tk.utils import PipValidationResult

BASE_FUNCTION: dict[str, Any] = {
    "externalId": "my_function",
    "name": "My Function",
    "dataSetExternalId": "my_dataset",
}


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
    def _create_built_resource(source_path: Path, build_path: Path, external_id: str = "my_function") -> BuiltResource:
        """Create a BuiltResource for testing."""
        return BuiltResource(
            identifier=ExternalId(external_id=external_id),
            source_hash="test-hash",
            type=ResourceType(resource_folder=FunctionIO.folder_name, kind=FunctionIO.kind),
            source_path=source_path.resolve(),
            build_path=build_path.resolve(),
            crud_cls=FunctionIO,
            dependencies=set(),
            has_syntax_error=False,
            module_id=ModuleId(
                id=Path("modules/my"),
                path=source_path.parent.resolve(),
            ),
        )

    def _create_function_resource(self, tmp_path: Path, extra: dict[str, Any] | None = None) -> BuiltResource:
        """Write a function YAML (base definition + extra fields) and wrap it in a BuiltResource."""
        yaml_file = tmp_path / "functions" / "func.yaml"
        self._write_function_yaml(yaml_file, {**BASE_FUNCTION, **(extra or {})})
        return self._create_built_resource(yaml_file, yaml_file)

    @pytest.mark.parametrize(
        "client_fixture, expected_code, expected_message_substring",
        [
            pytest.param("toolkit_client_cheap", "ready", "validate function limits", id="with client"),
            pytest.param(None, "reduced", "requirement", id="without client"),
        ],
    )
    def test_get_status(
        self,
        client_fixture: str | None,
        expected_code: str,
        expected_message_substring: str,
        request: pytest.FixtureRequest,
    ) -> None:
        """Test get_status depending on whether a client is available."""
        client = request.getfixturevalue(client_fixture) if client_fixture else None
        rule = FunctionRuleSet(modules=[], client=client)
        status = rule.get_status()
        assert status.code == expected_code
        assert status.message is not None
        assert expected_message_substring in status.message.lower()

    @pytest.mark.parametrize(
        "extra, expected_codes, expected_message_substrings",
        [
            pytest.param({"cpu": 3.0}, ["EXCEEDED-LIMIT"], ["CPU cores", "3.0"], id="cpu exceeds max"),
            pytest.param({"cpu": 0.05}, ["EXCEEDED-LIMIT"], ["CPU cores"], id="cpu below min"),
            pytest.param({"cpu": 5.0}, ["EXCEEDED-LIMIT"], ["CPU cores", "5.0"], id="cpu far exceeds max"),
            pytest.param({"memory": 5.0}, ["EXCEEDED-LIMIT"], ["memory"], id="memory exceeds max"),
            pytest.param({"memory": 0.1}, ["EXCEEDED-LIMIT"], ["memory"], id="memory below min"),
            pytest.param({"cpu": 1.0, "memory": 2.0}, [], [], id="valid resources"),
            pytest.param({"runtime": "py314"}, ["INVALID-VALUE"], ["py314"], id="unsupported runtime"),
            pytest.param({"runtime": "py312"}, [], [], id="supported runtime"),
            pytest.param({}, [], [], id="no resources specified"),
            pytest.param(
                {"cpu": 3.0, "memory": 5.0},
                ["EXCEEDED-LIMIT", "EXCEEDED-LIMIT"],
                ["CPU cores", "memory"],
                id="multiple violations",
            ),
        ],
    )
    def test_validate_function_limits(
        self,
        extra: dict[str, Any],
        expected_codes: list[str],
        expected_message_substrings: list[str],
        tmp_path: Path,
        function_limits: FunctionLimits,
    ) -> None:
        """Test validation of function definitions against the project limits."""
        resource = self._create_function_resource(tmp_path, extra)

        errors = list(FunctionRuleSet._validate_function(resource, function_limits))

        assert [error.code for error in errors] == expected_codes
        assert all(isinstance(error, BuildError) for error in errors)
        assert all(error.message and error.fix for error in errors)
        combined_messages = "\n".join(error.message for error in errors)
        for substring in expected_message_substrings:
            assert substring in combined_messages

    @pytest.mark.parametrize(
        "requirements_txt, pip_result, expected_codes",
        [
            pytest.param(
                "nonexistent-package==99.99.99\n",
                PipValidationResult(error_message="Package not found: nonexistent-package"),
                ["INVALID-FUNCTION-REQUIREMENTS"],
                id="invalid requirements.txt",
            ),
            pytest.param(
                "requests>=2.0.0\n",
                PipValidationResult(),
                [],
                id="valid requirements.txt",
            ),
            pytest.param(None, None, [], id="no requirements.txt"),
        ],
    )
    @patch("cognite_toolkit._cdf_tk.rules._functions.validate_requirements_with_pip")
    def test_validate_function_requirements(
        self,
        mock_validate_pip: MagicMock,
        requirements_txt: str | None,
        pip_result: PipValidationResult | None,
        expected_codes: list[str],
        tmp_path: Path,
        function_limits: FunctionLimits,
    ) -> None:
        """Test validation of the function's requirements.txt."""
        if requirements_txt is not None:
            function_dir = tmp_path / "functions" / "my_function"
            function_dir.mkdir(parents=True, exist_ok=True)
            (function_dir / "requirements.txt").write_text(requirements_txt)
        mock_validate_pip.return_value = pip_result

        resource = self._create_function_resource(tmp_path)
        errors = list(FunctionRuleSet._validate_function(resource, function_limits))

        assert [error.code for error in errors] == expected_codes
        if requirements_txt is None:
            mock_validate_pip.assert_not_called()
        else:
            mock_validate_pip.assert_called_once()

    @pytest.mark.parametrize(
        "missing_acls, activation_status, expected_codes, expected_message_substrings, expect_status_called",
        [
            pytest.param([], "activated", [], [], True, id="access and activated"),
            pytest.param([], "inactive", ["INVALID-VALUE"], ["activated", "inactive"], True, id="inactive"),
            pytest.param([], "requested", ["INVALID-VALUE"], ["activated", "requested"], True, id="requested"),
            pytest.param(
                [FunctionsAcl(actions=["READ"], scope=AllScope())],
                "activated",
                ["MISSING-READ-ACCESS"],
                ["READ access to Functions"],
                False,
                id="missing access",
            ),
        ],
    )
    def test_validate_function_service(
        self,
        missing_acls: list[FunctionsAcl],
        activation_status: str,
        expected_codes: list[str],
        expected_message_substrings: list[str],
        expect_status_called: bool,
        tmp_path: Path,
    ) -> None:
        """Test validation of access to and activation of the Cognite Functions service."""
        client = MagicMock()
        client.token.verify_acls.return_value = missing_acls
        client.functions.status.return_value = FunctionsActivation(status=activation_status)
        first_function = tmp_path / "functions" / "func.yaml"

        errors = list(FunctionRuleSet._validate_function_service(client, first_function))

        assert [error.code for error in errors] == expected_codes
        assert all(isinstance(error, BuildError) for error in errors)
        assert all(error.message and error.fix for error in errors)
        assert all(error.source_file == first_function for error in errors)
        combined_messages = "\n".join(error.message for error in errors)
        for substring in expected_message_substrings:
            assert substring in combined_messages
        assert client.functions.status.called is expect_status_called
