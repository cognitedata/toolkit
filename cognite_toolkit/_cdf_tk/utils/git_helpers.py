import subprocess


class Git:
    @classmethod
    def is_git_repo(cls) -> bool:
        """Check if the current directory is a git repository."""
        try:
            subprocess.run(
                ["git", "rev-parse", "--is-inside-work-tree"],
                capture_output=True,
                text=True,
                check=True,
            )
            return True
        except subprocess.CalledProcessError:
            return False

    @classmethod
    def get_default_branch(cls) -> str:
        """Get the default branch of the current git repository."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get default branch: {e.stderr.strip()}") from e

    @classmethod
    def get_current_branch(cls) -> str:
        """Get the current branch of the current git repository."""
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get current branch: {e.stderr.strip()}") from e

    @classmethod
    def get_changed_files(cls, base_branch: str, target_branch: str) -> list[str]:
        """Get the list of changed files between two branches."""
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only", f"{base_branch}..{target_branch}"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip().splitlines()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get changed files: {e.stderr.strip()}") from e

    @classmethod
    def get_changed_files_last_commit(cls) -> list[str]:
        """Get the list of changed files in the last commit."""
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only", "HEAD~1..HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip().splitlines()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get changed files in last commit: {e.stderr.strip()}") from e

    @classmethod
    def get_uncommitted_changes(cls) -> list[str]:
        """Get the list of uncommitted changes in the current working directory."""
        try:
            result = subprocess.run(
                ["git", "diff", "--name-only"],
                capture_output=True,
                text=True,
                check=True,
            )
            return result.stdout.strip().splitlines()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get uncommitted changes: {e.stderr.strip()}") from e
