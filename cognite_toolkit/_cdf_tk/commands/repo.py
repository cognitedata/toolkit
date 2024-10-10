import shutil
from importlib import resources
from pathlib import Path

import questionary

import cognite_toolkit
from cognite_toolkit._cdf_tk.constants import REPO_FILES_DIR
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.tk_warnings import LowSeverityWarning, MediumSeverityWarning

from . import _cli_commands
from ._base import ToolkitCommand

REPOSITORY_HOSTING = [
    "GitHub",
    "Azure DevOps",
    "Other",
    "None",
]


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

    def init(self, cwd: Path, host: str | None = None, verbose: bool = False) -> None:
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

        if host is None:
            repo_host = questionary.select("Where do are you hosting the repository?", REPOSITORY_HOSTING).ask()
        else:
            repo_host = host
        if repo_host == "GitHub":
            self.console("The repository will be hosted on GitHub.")
        elif repo_host == "Azure DevOps":
            self.console("The repository will be hosted on Azure DevOps.")
        elif repo_host == "Other":
            self.console("No template for CI/CD available for other hosting services yet.")
        elif repo_host == "None":
            self.console("It is recommended to use a hosted version control service like GitHub or Azure DevOps.")

        if verbose:
            self.console("Initializing toolkit repository...")

        iterables = [(self._repo_files, self._repo_files.glob("*"))]
        if repo_host in ["GitHub", "Azure DevOps"]:
            repo_host = repo_host.replace(" ", "")
            iterables.append((self._repo_files / repo_host, self._repo_files.rglob(f"{repo_host}/**/*.*")))

        for root, iterable in iterables:
            for file in iterable:
                if file.is_dir():
                    continue
                destination = cwd / file.relative_to(root)
                if destination.exists():
                    self.warn(LowSeverityWarning(f"File {destination} already exists. Skipping..."))
                    continue
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy(file, destination)
                if verbose:
                    self.console(f"Created {destination.relative_to(cwd).as_posix()!r}")

        self.console("Repo initialization complete.")
