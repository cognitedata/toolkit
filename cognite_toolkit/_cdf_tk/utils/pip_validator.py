"""Utilities for validating Python package requirements."""

import re
import subprocess
import sys
from pathlib import Path


def has_template_variables(url: str | None) -> bool:
    """Check if a URL contains template variables that haven't been resolved yet."""
    if not url:
        return False
    patterns = [  # Pattern matches common template variable formats
        r"\{\{[^}]+\}\}",  # {{VARIABLE}}
        r"\$\{[^}]+\}",  # ${VARIABLE}
        r"\$[A-Z_][A-Z0-9_]*",  # $VARIABLE
        r"\{[A-Z_][A-Z0-9_]*\}",  # {VARIABLE}
    ]
    return any(re.search(pattern, url) for pattern in patterns)


class PipValidationResult:
    """Result from validating a requirements.txt file."""

    def __init__(
        self,
        success: bool,
        error_message: str | None = None,
        stderr: str | None = None,
        skipped: bool = False,
        skip_reason: str | None = None,
    ) -> None:
        self.success = success
        self.error_message = error_message
        self.stderr = stderr
        self.skipped = skipped
        self.skip_reason = skip_reason

    @property
    def is_credential_error(self) -> bool:
        """Check if the error appears to be related to authentication/credentials."""
        if not self.stderr:
            return False
        credential_indicators = [
            "401",
            "403",
            "Unauthorized",
            "Authentication",
            "credentials",
            "Could not find a version that satisfies the requirement",
            "No matching distribution found",
        ]
        return any(indicator.lower() in self.stderr.lower() for indicator in credential_indicators)


def validate_requirements_with_pip(
    requirements_txt_path: Path,
    index_url: str | None = None,
    extra_index_urls: list[str] | None = None,
    timeout: int = 30,
) -> PipValidationResult:
    """
    Validate that requirements.txt can be resolved using pip install --dry-run.

    This simulates package installation without actually installing anything.
    It validates that:
    - All packages exist
    - All specified versions are available
    - Credentials for private repositories are valid
    - Index URLs are accessible

    Important: Validation is skipped if index URLs contain unresolved template variables
    (e.g., {{PAT}}, ${SECRET}). This commonly happens when credentials are injected
    during CI/CD but aren't available during local development.

    Args:
        requirements_txt_path: Path to the requirements.txt file
        index_url: Optional custom package index URL (replaces PyPI)
        extra_index_urls: Optional additional package index URLs
        timeout: Timeout in seconds for the pip command

    Returns:
        PipValidationResult with success status and error details if failed,
        or skipped=True if validation couldn't be performed
    """
    # Early validation checks
    if not requirements_txt_path.exists():
        return PipValidationResult(success=False, error_message=f"Requirements file not found: {requirements_txt_path}")

    if has_template_variables(index_url):
        return PipValidationResult(
            success=True, skipped=True, skip_reason=f"indexUrl contains unresolved template variables: {index_url}"
        )

    if extra_index_urls and (unresolved := [url for url in extra_index_urls if has_template_variables(url)]):
        return PipValidationResult(
            success=True,
            skipped=True,
            skip_reason=f"extraIndexUrls contain unresolved template variables: {', '.join(unresolved)}",
        )

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
        return (
            PipValidationResult(success=True)
            if result.returncode == 0
            else PipValidationResult(
                success=False,
                error_message=f"pip validation failed with exit code {result.returncode}",
                stderr=result.stderr,
            )
        )
    except subprocess.TimeoutExpired:
        return PipValidationResult(success=False, error_message=f"pip validation timed out after {timeout} seconds")
    except Exception as e:
        return PipValidationResult(success=False, error_message=f"Unexpected error during pip validation: {e!s}")
