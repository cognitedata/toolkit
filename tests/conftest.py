"""
Root conftest.py for shared test fixtures.

Provides worker-scoped fixtures for external library caching to avoid race conditions
and expensive repeated downloads when running tests with pytest-xdist.
"""

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

from cognite_toolkit._cdf_tk.commands.modules import ModulesCommand
from cognite_toolkit._cdf_tk.utils import sanitize_filename

# Global registry to track temp directories for cleanup
_TEMP_DIRS_TO_CLEANUP: set[Path] = set()


def _cleanup_temp_dirs() -> None:
    """Clean up all registered temp directories at process exit."""
    for temp_dir in _TEMP_DIRS_TO_CLEANUP:
        if temp_dir.exists():
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception as e:
                print(f"Warning: Failed to clean up temp directory {temp_dir}: {e}")


atexit.register(_cleanup_temp_dirs)


def get_worker_id() -> str:
    """Get the current pytest-xdist worker ID, or 'master' if not using xdist."""
    worker_id = os.environ.get("PYTEST_XDIST_WORKER", "master")
    return sanitize_filename(worker_id)


@contextmanager
def external_library_cache_context(cache_root: Path) -> Iterator[Path]:
    """Context manager for accessing the external library cache with file locking."""
    worker_id = get_worker_id()
    cache_dir = cache_root / f"modules-{worker_id}"
    lock_file = cache_root / f"modules-{worker_id}.lock"

    cache_dir.mkdir(parents=True, exist_ok=True)
    with FileLock(lock_file, timeout=300):
        yield cache_dir


def get_unique_run_id() -> str:
    """Get a unique identifier for this GitHub Actions job run."""
    run_id = os.getenv("GITHUB_RUN_ID", "")
    run_attempt = os.getenv("GITHUB_RUN_ATTEMPT", "")
    job_id = os.getenv("GITHUB_JOB", "")

    if run_id:
        parts = [run_id]
        if run_attempt:
            parts.append(run_attempt)
        if job_id:
            job_id_safe = sanitize_filename(job_id)
            parts.append(job_id_safe)
        return "-".join(parts)

    import time

    return f"local-{os.getpid()}-{int(time.time() * 1000)}"


@pytest.fixture(scope="session")
def external_library_cache_root() -> Iterator[Path]:
    """Session-scoped fixture providing a root directory for external library caching."""
    run_id = get_unique_run_id()
    session_id = os.getpid()
    cache_root = Path(tempfile.gettempdir()) / f"toolkit-test-cache-{run_id}-{session_id}"
    cache_root.mkdir(parents=True, exist_ok=True)

    _TEMP_DIRS_TO_CLEANUP.add(cache_root)

    yield cache_root

    if cache_root.exists():
        shutil.rmtree(cache_root, ignore_errors=True)


@pytest.fixture(scope="session")
def external_library_modules_dir(external_library_cache_root: Path) -> Iterator[Path]:
    """Session-scoped fixture providing a worker-specific directory for external library downloads."""
    worker_id = get_worker_id()
    modules_dir = external_library_cache_root / f"modules-{worker_id}"
    modules_dir.mkdir(parents=True, exist_ok=True)

    yield modules_dir


@pytest.fixture(scope="function")
def isolated_external_library_dir(external_library_cache_root: Path) -> Iterator[Path]:
    """Function-scoped fixture for tests that need a completely isolated library directory."""
    test_id = os.environ.get("PYTEST_CURRENT_TEST", "unknown")
    test_hash = hashlib.sha256(test_id.encode()).hexdigest()[:8]
    worker_id = get_worker_id()

    isolated_dir = external_library_cache_root / f"isolated-{worker_id}-{test_hash}"
    isolated_dir.mkdir(parents=True, exist_ok=True)

    yield isolated_dir

    if isolated_dir.exists():
        shutil.rmtree(isolated_dir, ignore_errors=True)


@pytest.fixture(scope="function")
def modules_command_with_cached_download(
    external_library_cache_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> type:
    """Function-scoped fixture providing a pre-configured ModulesCommand class with caching."""
    worker_id = get_worker_id()
    cache_dir = external_library_cache_root / worker_id / "modules"
    cache_dir.mkdir(parents=True, exist_ok=True)

    lock_file = external_library_cache_root / worker_id / "modules.lock"

    original_init = ModulesCommand.__init__
    original_download = ModulesCommand._download
    original_unpack = ModulesCommand._unpack

    def patched_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        self._temp_download_dir = cache_dir

    def patched_download(self, url: str, file_path: Path) -> None:
        """Download with caching and locking - skip if file already exists."""
        with FileLock(lock_file, timeout=300):
            if file_path.exists():
                if file_path.stat().st_size > 0:
                    return

            unpacked_dir = file_path.parent / file_path.stem
            if unpacked_dir.exists() and unpacked_dir.is_dir():
                try:
                    if any(unpacked_dir.iterdir()):
                        return
                except OSError as e:
                    print(f"Warning: Could not check unpacked directory {unpacked_dir}: {e}")

            original_download(self, url, file_path)

    def patched_unpack(self, file_path: Path) -> None:
        """Unpack with locking to prevent race conditions."""
        with FileLock(lock_file, timeout=300):
            unpacked_dir = file_path.parent / file_path.stem
            if unpacked_dir.exists() and unpacked_dir.is_dir():
                try:
                    if any(unpacked_dir.iterdir()):
                        return
                except OSError as e:
                    print(f"Warning: Could not check unpacked directory {unpacked_dir}: {e}")

            original_unpack(self, file_path)

    def patched_exit(self, *args, **kwargs):
        """Prevent cleanup - cache persists until session end."""
        pass

    monkeypatch.setattr(ModulesCommand, "__init__", patched_init)
    monkeypatch.setattr(ModulesCommand, "_download", patched_download)
    monkeypatch.setattr(ModulesCommand, "_unpack", patched_unpack)
    monkeypatch.setattr(ModulesCommand, "__exit__", patched_exit)

    return ModulesCommand
