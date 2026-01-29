"""Utilities for validating Python package requirements."""

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipValidationResult:
    """Result from validating a requirements.txt file."""

    error_message: str | None = None
    # The number of lines to include in the short error message
    max_error_lines: int = 3

    @property
    def success(self) -> bool:
        """Validation succeeded if there's no error message."""
        return self.error_message is None

    @property
    def is_credential_error(self) -> bool:
        """Check if the error appears to be related to authentication/credentials.

        Only checks for explicit HTTP authentication errors to avoid false positives
        from legitimate package not found errors.
        """
        if not self.error_message:
            return False
        credential_indicators = [
            "401",
            "403",
            "Unauthorized",
            "Forbidden",
            "Authentication",
        ]
        return any(indicator in self.error_message for indicator in credential_indicators)

    @property
    def short_error(self) -> str:
        """Get a shortened version of the error message with limited lines."""
        error_detail = self.error_message or "Unknown error"
        relevant_lines = [line for line in error_detail.strip().split("\n") if line.strip()][-self.max_error_lines :]
        error_detail = "\n      ".join(relevant_lines)
        return error_detail


def validate_requirements_with_pip(
    requirements_txt_path: Path,
    index_url: str | None = None,
    extra_index_urls: list[str] | None = None,
    timeout: int = 10,
) -> PipValidationResult:
    """Validate that requirements.txt can be resolved using pip install --dry-run.

    This simulates package installation without actually installing anything.
    It validates that:
    - All packages exist
    - All specified versions are available
    - Credentials for private repositories are valid
    - Index URLs are accessible

    Args:
        requirements_txt_path: Path to the requirements.txt file
        index_url: Optional custom package index URL (replaces PyPI)
        extra_index_urls: Optional additional package index URLs
        timeout: Timeout in seconds for the pip command

    Returns:
        PipValidationResult with success status and error details if failed

    """
    if not requirements_txt_path.exists():
        return PipValidationResult(error_message=f"Requirements file not found: {requirements_txt_path}")

    # Build pip command with custom index URLs
    args = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--dry-run",
        "--ignore-installed",
        "--no-deps",
        "--disable-pip-version-check",
        "-r",
        str(requirements_txt_path),
        *(["--index-url", index_url] if index_url else []),
        *([arg for url in (extra_index_urls or []) for arg in ["--extra-index-url", url]]),
    ]

    try:
        result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
        if result.returncode == 0:
            return PipValidationResult()
        return PipValidationResult(
            error_message=f"pip validation failed with exit code {result.returncode}:\n{result.stderr}",
        )
    except subprocess.TimeoutExpired:
        return PipValidationResult(error_message=f"pip validation timed out after {timeout} seconds")
    except (OSError, RuntimeError) as e:
        return PipValidationResult(error_message=f"Error running pip validation: {e!s}")
