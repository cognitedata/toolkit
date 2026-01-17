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

    @pytest.mark.parametrize(
        "returncode,stderr,should_succeed",
        [
            (0, "", True),
            (1, "ERROR: Package not found", False),
        ],
    )
    def test_validation_result(
        self, requirements_file: Path, returncode: int, stderr: str, should_succeed: bool
    ) -> None:
        """Test validation success and failure cases."""
        with patch("subprocess.run", return_value=MagicMock(returncode=returncode, stderr=stderr, stdout="")):
            result = validate_requirements_with_pip(requirements_file)
        assert result.success == should_succeed
        if not should_succeed:
            assert result.error_message
            if stderr:
                assert stderr.replace("ERROR: ", "") in result.error_message

    @pytest.mark.parametrize(
        "index_url,extra_index_urls,expected_args",
        [
            ("https://custom.repo.com/simple", None, ["--index-url", "https://custom.repo.com/simple"]),
            (None, ["https://extra.com/simple"], ["--extra-index-url", "https://extra.com/simple"]),
            (
                "https://custom.com/simple",
                ["https://extra1.com/simple", "https://extra2.com/simple"],
                ["--index-url", "--extra-index-url"],
            ),
        ],
    )
    def test_custom_index_urls(
        self,
        requirements_file: Path,
        index_url: str | None,
        extra_index_urls: list[str] | None,
        expected_args: list[str],
    ) -> None:
        """Test that custom index URLs are passed to pip correctly."""
        with patch("subprocess.run", return_value=MagicMock(returncode=0, stderr="", stdout="")) as mock:
            result = validate_requirements_with_pip(
                requirements_file, index_url=index_url, extra_index_urls=extra_index_urls
            )
            for arg in expected_args:
                assert arg in mock.call_args[0][0], f"Expected {arg} in pip command"
        assert result.success

    def test_timeout(self, requirements_file: Path) -> None:
        with patch("subprocess.run", side_effect=TimeoutExpired("pip", 1)):
            result = validate_requirements_with_pip(requirements_file, timeout=1)
        assert not result.success
        assert result.error_message and "timed out" in result.error_message.lower()

    @pytest.mark.parametrize(
        "error_message,is_credential_error",
        [
            # Credential errors
            ("401 Unauthorized", True),
            ("403 Forbidden", True),
            ("Authentication required", True),
            ("ERROR: HTTP error 401 while downloading", True),
            ("Forbidden access to repository", True),
            # Non-credential errors
            ("Could not find a version that satisfies the requirement", False),
            ("No matching distribution found for package", False),
            ("ERROR: Package 'nonexistent-package' not found", False),
            ("Invalid requirement string", False),
        ],
    )
    def test_credential_error_detection(self, error_message: str, is_credential_error: bool) -> None:
        """Test credential error detection for various error patterns."""
        result = PipValidationResult(error_message=error_message)
        assert result.is_credential_error == is_credential_error, (
            f"'{error_message}' should{'' if is_credential_error else ' not'} be flagged as credential error"
        )
