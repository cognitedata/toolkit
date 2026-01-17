from pathlib import Path
from subprocess import TimeoutExpired
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.utils.pip_validator import (
    PipValidationResult,
    validate_requirements_with_pip,
)


@pytest.fixture
def requirements_file(tmp_path: Path) -> Path:
    """Create a requirements.txt file."""
    req_file = tmp_path / "requirements.txt"
    req_file.write_text("requests>=2.25.0\n")
    return req_file


class TestPipValidator:
    def test_requirements_file_not_found(self, tmp_path: Path) -> None:
        result = validate_requirements_with_pip(tmp_path / "nonexistent.txt")
        assert not result.success
        assert result.error_message and "not found" in result.error_message.lower()

    def test_successful_validation(self, requirements_file: Path) -> None:
        with patch("subprocess.run", return_value=MagicMock(returncode=0, stderr="", stdout="")):
            result = validate_requirements_with_pip(requirements_file)
        assert result.success
        assert not result.error_message

    def test_validation_failure(self, requirements_file: Path) -> None:
        with patch(
            "subprocess.run", return_value=MagicMock(returncode=1, stderr="ERROR: Package not found", stdout="")
        ):
            result = validate_requirements_with_pip(requirements_file)
        assert not result.success
        assert result.error_message
        assert "Package not found" in result.error_message

    def test_credential_error_detection(self, requirements_file: Path) -> None:
        with patch("subprocess.run", return_value=MagicMock(returncode=1, stderr="ERROR: 401 Unauthorized", stdout="")):
            result = validate_requirements_with_pip(requirements_file)
        assert not result.success
        assert result.is_credential_error

    def test_custom_index_url(self, requirements_file: Path) -> None:
        with patch("subprocess.run", return_value=MagicMock(returncode=0, stderr="", stdout="")) as mock:
            result = validate_requirements_with_pip(requirements_file, index_url="https://custom.repo.com/simple")
            assert "--index-url" in mock.call_args[0][0]
        assert result.success

    def test_extra_index_urls(self, requirements_file: Path) -> None:
        with patch("subprocess.run", return_value=MagicMock(returncode=0, stderr="", stdout="")) as mock:
            result = validate_requirements_with_pip(requirements_file, extra_index_urls=["https://extra.com/simple"])
            assert "--extra-index-url" in mock.call_args[0][0]
        assert result.success

    def test_timeout(self, requirements_file: Path) -> None:
        with patch("subprocess.run", side_effect=TimeoutExpired("pip", 1)):
            result = validate_requirements_with_pip(requirements_file, timeout=1)
        assert not result.success
        assert result.error_message and "timed out" in result.error_message.lower()

    @pytest.mark.parametrize(
        "error_pattern",
        [
            "401 Unauthorized",
            "403 Forbidden",
            "Authentication required",
            "Could not find a version that satisfies the requirement azure-functions==1.*",
            "No matching distribution found for package",
        ],
    )
    def test_authentication_error_patterns(self, error_pattern: str) -> None:
        result = PipValidationResult(error_message=error_pattern)
        assert result.is_credential_error
