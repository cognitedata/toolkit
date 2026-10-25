import pytest

from cognite_toolkit._cdf_tk.utils.cicd import get_cicd_environment
from cognite_toolkit._cdf_tk.utils.git_helpers import Git


@pytest.fixture(scope="session")
def git() -> Git:
    """Fixture to provide a Git instance."""
    return Git()


class TestGit:
    def test_is_git_repo(self, git: Git) -> None:
        """Test that the current directory is a git repository."""
        assert git.is_git_repo(), "Current directory is not a git repository."

    @pytest.mark.skipif(
        get_cicd_environment() == "local", reason="This tests only works in CI/CD environments, not locally."
    )
    def test_get_current_branch(self, git: Git) -> None:
        """Test that the current branch can be retrieved."""
        assert git.get_current_branch() == "main", 'Smoke tests should be run from the "main" branch.'

    def test_get_default_branch(self, git: Git) -> None:
        """Test that the default branch can be retrieved."""
        assert git.get_default_branch() == "main", 'Smoke tests should be run from the "main" branch.'

    def test_get_uncommitted_changes(self, git: Git) -> None:
        """Test that there are no uncommitted changes."""
        uncommitted_changes = git.get_uncommitted_changes()
        assert not uncommitted_changes, f"There are uncommitted changes:\n{uncommitted_changes}"

    def test_get_changed_files_since_commit(self, git: Git) -> None:
        """Test that there are no changed files sdince the last commit."""
        changed_files = git.get_changed_files_last_commit()
        print(f"Changed files since last commit: {changed_files}")
        assert changed_files, f"There are changed files since the last commit:\n{changed_files}"
