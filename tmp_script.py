from cognite_toolkit._cdf_tk.utils.git_helpers import Git

git = Git()
try:
    print(f"{git.get_me()=}")
except Exception as e:
    print(f"Error getting git user: {e}")

try:
    print(f"{git.is_git_repo()=}")
except Exception as e:
    print(f"Error checking if git repo: {e}")

try:
    print(f"{git.get_current_branch()=}")
except Exception as e:
    print(f"Error getting current branch: {e}")

try:
    print(f"{git.get_default_branch()=}")
except Exception as e:
    print(f"Error getting default branch: {e}")


try:
    print(f"{git.get_uncommitted_changes()=}")
except Exception as e:
    print(f"Error getting uncommitted changes: {e}")

try:
    print(f"{git.get_changed_files_last_commit()=}")
except Exception as e:
    print(f"Error getting changed files in last commit: {e}")
