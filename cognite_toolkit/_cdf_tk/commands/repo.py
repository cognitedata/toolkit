import shutil
from importlib import resources
from pathlib import Path

import cognite_toolkit
from cognite_toolkit._cdf_tk.constants import REPO_FILES_DIR
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning, MediumSeverityWarning

from . import _cli_commands
from ._base import ToolkitCommand


class RepoCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning=print_warning, skip_tracking=skip_tracking, silent=silent)
        self._repo_files = Path(resources.files(cognite_toolkit.__name__)) / REPO_FILES_DIR  # type: ignore [arg-type]

    def init(self, cwd: Path, verbose: bool = False) -> None:
        if (git_root := _cli_commands.git_root()) is None:
            if not _cli_commands.use_git():
                self.warn(
                    MediumSeverityWarning("git is not installed. It is strongly recommended to use version control.")
                )
            elif not _cli_commands.has_initiated_repo():
                self.warn(MediumSeverityWarning("git repository not initiated. Did you forget to run `git init`?"))
            else:
                self.warn(MediumSeverityWarning("Unknown error when trying to find git root."))
        if cwd != git_root:
            self.warn(
                MediumSeverityWarning(
                    f"Current working directory is not the root of the git repository. "
                    f"Please run this command from {git_root}."
                )
            )
            return None

        if verbose:
            self.console("Initializing git repository...")
            self.console(
                f"Will create {[abs_file.relative_to(self._repo_files) for abs_file in self._repo_files.iterdir()]} in {cwd}"
            )

        for file in self._repo_files.rglob("*"):
            destination = cwd / file.relative_to(self._repo_files)
            if destination.exists():
                self.warn(LowSeverityWarning(f"File {destination} already exists. Skipping..."))
                continue
            shutil.copy(file, destination)
            if verbose:
                self.console(f"Created {destination}")
        self.console("Repo initialization complete.")
