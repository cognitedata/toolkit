import shutil
from importlib import resources
from pathlib import Path

import cognite_toolkit
from cognite_toolkit._cdf_tk.constants import REPO_FILES_DIR
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning, MediumSeverityWarning

from . import _cli_commands
from ._base import ToolkitCommand


class RepoCommand(ToolkitCommand):
    def __init__(
        self,
        print_warning: bool = True,
        skip_tracking: bool = False,
        silent: bool = False,
        skip_git_verify: bool = False,
    ) -> None:
        super().__init__(print_warning=print_warning, skip_tracking=skip_tracking, silent=silent)
        self._repo_files = Path(resources.files(cognite_toolkit.__name__)) / REPO_FILES_DIR  # type: ignore [arg-type]
        self.skip_git_verify = skip_git_verify

    def init(self, cwd: Path, verbose: bool = False) -> None:
        if not self.skip_git_verify:
            if _cli_commands.use_git():
                if not _cli_commands.has_initiated_repo():
                    self.warn(MediumSeverityWarning("git repository not initiated. Did you forget to run `git init`?"))
                else:
                    git_root = _cli_commands.git_root()
                    if git_root is None:
                        self.warn(MediumSeverityWarning("Unknown error when trying to find git root."))
                    elif cwd != git_root:
                        raise ToolkitValueError(
                            f"Current working directory is not the root of the git repository. "
                            f"Please run this command from {git_root.as_posix()!r}."  # type: ignore [union-attr]
                        )
            else:
                self.warn(
                    MediumSeverityWarning("git is not installed. It is strongly recommended to use version control.")
                )

        if verbose:
            self.console("Initializing toolkit repository...")

        for file in self._repo_files.rglob("*"):
            destination = cwd / file.relative_to(self._repo_files)
            if destination.exists():
                self.warn(LowSeverityWarning(f"File {destination} already exists. Skipping..."))
                continue
            shutil.copy(file, destination)
            if verbose:
                self.console(f"Created {destination.relative_to(cwd).as_posix()!r}")
        self.console("Repo initialization complete.")
