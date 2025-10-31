"""
Root conftest.py for shared test fixtures across unit and integration tests.

This module provides worker-scoped fixtures for external library caching to avoid
race conditions and expensive repeated downloads when running tests with pytest-xdist.
"""

from __future__ import annotations

import atexit
import hashlib
import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from filelock import FileLock

# Global registry to track temp directories for cleanup
_TEMP_DIRS_TO_CLEANUP: set[Path] = set()


def _cleanup_temp_dirs() -> None:
    """Clean up all registered temp directories at process exit."""
    for temp_dir in _TEMP_DIRS_TO_CLEANUP:
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass  # Best effort cleanup


# Register cleanup handler
atexit.register(_cleanup_temp_dirs)


def get_worker_id() -> str:
    """
    Get the current pytest-xdist worker ID, or 'master' if not using xdist.

    Returns a sanitized worker ID that can be used in directory names.
    """
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
    # Sanitize the worker ID to be filesystem-safe
    return worker_id.replace("/", "_").replace("\\", "_")


@contextmanager
def external_library_cache_context(cache_root: Path) -> Iterator[Path]:
    """
    Context manager for accessing the external library cache with file locking.

    This ensures that only one process/thread can download libraries at a time,
    preventing race conditions when multiple tests start simultaneously.

    Args:
        cache_root: Root directory for the cache

    Yields:
        Path to the modules cache directory for this worker
    """
    worker_id = get_worker_id()
    cache_dir = cache_root / f"modules-{worker_id}"
    lock_file = cache_root / f"modules-{worker_id}.lock"

    # Create cache directory if it doesn't exist
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Use file lock to prevent race conditions
    with FileLock(lock_file, timeout=300):  # 5 minute timeout
        yield cache_dir


def get_unique_run_id() -> str:
    """
    Get a unique identifier for this GitHub Actions job run.

    Uses GitHub Actions environment variables to create a unique identifier per matrix job:
    - GITHUB_RUN_ID: Same for all matrix jobs in the same workflow run
    - GITHUB_RUN_ATTEMPT: Unique per workflow run attempt
    - GITHUB_JOB: Includes matrix values, unique per matrix job (e.g., "test-py310-ubuntu")

    This ensures each matrix job gets its own cache directory, preventing conflicts
    when multiple matrix jobs run in parallel.

    Example:
        Matrix job "test-py310-ubuntu" in run 12345, attempt 1:
        Returns: "12345-1-test-py310-ubuntu"
    """
    run_id = os.getenv("GITHUB_RUN_ID", "")
    run_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "")
    job_id = os.getenv("GITHUB_JOB", "")

    if run_id:
        parts = [run_id]
        if run_attempt:
            parts.append(run_attempt)
        if job_id:
            # Sanitize job ID to be filesystem-safe
            job_id_safe = job_id.replace("/", "_").replace("\\", "_").replace(" ", "_")
            parts.append(job_id_safe)
        return "-".join(parts)

    # Fallback for local development: use PID + timestamp
    import time

    return f"local-{os.getpid()}-{int(time.time() * 1000)}"


@pytest.fixture(scope="session")
def external_library_cache_root() -> Iterator[Path]:
    """
    Session-scoped fixture providing a root directory for external library caching.

    This creates a unique cache directory for the test session that survives across
    all tests but is cleaned up at the end of the session. The cache directory includes
    GitHub Actions run information to prevent conflicts when multiple matrix jobs run in parallel.

    The cache directory name includes:
    - GITHUB_RUN_ID: Identifies the workflow run
    - GITHUB_RUN_ATTEMPT: Identifies the attempt number
    - GITHUB_JOB: Includes matrix values, making each matrix job unique
    - Process ID: Additional uniqueness within the same job

    Example cache directory:
        /tmp/toolkit-test-cache-12345-1-test-py310-ubuntu-1234/

    Usage in tests:
        This fixture is automatically available to other fixtures that need it.
        Tests should use `external_library_modules_dir` instead of this directly.
    """
    # Create a unique cache root based on CI run + session
    run_id = get_unique_run_id()
    session_id = os.getpid()
    cache_root = Path(tempfile.gettempdir()) / f"toolkit-test-cache-{run_id}-{session_id}"
    cache_root.mkdir(parents=True, exist_ok=True)

    # Register for cleanup
    _TEMP_DIRS_TO_CLEANUP.add(cache_root)

    yield cache_root

    # Cleanup at end of session
    if cache_root.exists():
        shutil.rmtree(cache_root, ignore_errors=True)


@pytest.fixture(scope="session")
def external_library_modules_dir(external_library_cache_root: Path) -> Iterator[Path]:
    """
    Session-scoped fixture providing a worker-specific directory for external library downloads.

    This fixture provides a safe, worker-isolated directory for downloading and unpacking
    external libraries. It uses file locking to prevent race conditions and caches
    downloaded libraries to avoid repeated downloads.

    Benefits:
        - Avoids race conditions between test workers
        - Caches downloads across tests within a worker
        - Automatically cleans up at session end
        - Works with both sequential and parallel (pytest-xdist) test execution

    Usage in tests:
        def test_with_external_libraries(external_library_modules_dir, monkeypatch):
            # Override the temp download directory for ModulesCommand
            monkeypatch.setattr(
                "cognite_toolkit._cdf_tk.commands.modules.ModulesCommand._temp_download_dir",
                external_library_modules_dir,
                raising=False
            )
            # ... rest of test

    Example with context manager:
        def test_that_needs_download(external_library_modules_dir):
            from cognite_toolkit._cdf_tk.commands import ModulesCommand

            with monkeypatch.context() as m:
                # Ensure ModulesCommand uses our cache directory
                m.setattr("tempfile.gettempdir", lambda: str(external_library_modules_dir.parent))

                with ModulesCommand() as cmd:
                    # This will now use the cached directory
                    packages, location = cmd._get_available_packages()
    """
    worker_id = get_worker_id()
    modules_dir = external_library_cache_root / f"modules-{worker_id}"
    modules_dir.mkdir(parents=True, exist_ok=True)

    yield modules_dir

    # Note: We don't clean up here because it's session-scoped
    # The external_library_cache_root fixture handles cleanup


@pytest.fixture(scope="function")
def isolated_external_library_dir(external_library_cache_root: Path) -> Iterator[Path]:
    """
    Function-scoped fixture for tests that need a completely isolated library directory.

    This should be used for tests that modify or delete library files and need
    complete isolation from other tests. Most tests should use the session-scoped
    `external_library_modules_dir` instead for better performance.

    Usage:
        def test_with_isolation(isolated_external_library_dir, monkeypatch):
            # This test gets its own directory that won't affect other tests
            monkeypatch.setattr(...)
    """
    # Create a unique directory for this test using a hash of test details
    test_id = os.environ.get("PYTEST_CURRENT_TEST", "unknown")
    test_hash = hashlib.sha256(test_id.encode()).hexdigest()[:8]
    worker_id = get_worker_id()

    isolated_dir = external_library_cache_root / f"isolated-{worker_id}-{test_hash}"
    isolated_dir.mkdir(parents=True, exist_ok=True)

    yield isolated_dir

    # Clean up after the test
    if isolated_dir.exists():
        shutil.rmtree(isolated_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def modules_command_with_cached_download(
    external_library_cache_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> type:
    """
    Function-scoped fixture that provides a pre-configured ModulesCommand class with caching.

    This is the RECOMMENDED fixture for most tests that use external libraries.
    It automatically:
    - Uses worker-specific cache directories (shared across tests in the same worker)
    - Prevents cleanup of cached files (cleanup happens at session end)
    - Skips downloads if files already exist
    - Uses file locking to prevent race conditions during downloads

    Benefits:
        - Only patches ModulesCommand for tests that use this fixture
        - Race condition safe with proper locking
        - Fast - downloads once per worker, then caches
        - Works with pytest-xdist parallel execution
        - Cache persists across tests in the same worker

    Usage:
        def test_something(modules_command_with_cached_download):
            # Simply use the ModulesCommand class - it's already configured!
            with modules_command_with_cached_download() as cmd:
                packages, location = cmd._get_available_packages()
                assert packages is not None

    Note:
        This fixture is function-scoped, so each test gets its own patched version.
        However, all tests share the same cache directory (per worker), so downloads
        are cached across tests.
    """
    from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand

    worker_id = get_worker_id()
    cache_dir = external_library_cache_root / worker_id / "modules"
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Create lock file for this worker's cache directory
    lock_file = external_library_cache_root / worker_id / "modules.lock"

    # Store original methods (before any other patches)
    original_init = ModulesCommand.__init__
    original_download = ModulesCommand._download
    original_unpack = ModulesCommand._unpack

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self._temp_download_dir = cache_dir

    def patched_download(self, url: str, file_path: Path) -> None:
        """Download with caching and locking - skip if file already exists."""
        # Use file lock to prevent simultaneous downloads
        with FileLock(lock_file, timeout=300):
            # Double-check after acquiring lock (another test might have downloaded)
            if file_path.exists():
                if file_path.stat().st_size > 0:
                    return  # File already downloaded, skip

            # Also check if unpacked directory exists
            unpacked_dir = file_path.parent / file_path.stem
            if unpacked_dir.exists() and unpacked_dir.is_dir():
                try:
                    if any(unpacked_dir.iterdir()):
                        return  # Already unpacked, skip download
                except OSError:
                    pass

            # File doesn't exist or is invalid, proceed with download
            original_download(self, url, file_path)

    def patched_unpack(self, file_path: Path) -> None:
        """Unpack with locking to prevent race conditions."""
        # Use file lock to prevent simultaneous unpacking
        with FileLock(lock_file, timeout=300):
            # Check if already unpacked
            unpacked_dir = file_path.parent / file_path.stem
            if unpacked_dir.exists() and unpacked_dir.is_dir():
                try:
                    if any(unpacked_dir.iterdir()):
                        return  # Already unpacked, skip
                except OSError:
                    pass

            # Proceed with unpack
            original_unpack(self, file_path)

    def patched_exit(self, *args, **kwargs):
        """Prevent cleanup - cache persists until session end."""
        # Don't delete the temp directory - let session cleanup handle it
        pass

    # Patch only for this test (function-scoped)
    monkeypatch.setattr(ModulesCommand, "__init__", patched_init)
    monkeypatch.setattr(ModulesCommand, "_download", patched_download)
    monkeypatch.setattr(ModulesCommand, "_unpack", patched_unpack)
    monkeypatch.setattr(ModulesCommand, "__exit__", patched_exit)

    # Return the class - tests can use it normally
    return ModulesCommand
