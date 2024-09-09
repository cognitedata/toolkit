from importlib import resources
from pathlib import Path

import cognite_toolkit
from cognite_toolkit._cdf_tk.constants import REPO_FILES

from ._base import ToolkitCommand


class RepoCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning=print_warning, skip_tracking=skip_tracking, silent=silent)
        self._repo_files = Path(resources.files(cognite_toolkit.__name__)) / REPO_FILES  # type: ignore [arg-type]

    def init(self, cwd: Path) -> None:
        print("Initializing git repository...")
        print(f"Will create {list(self._repo_files.iterdir())} in {cwd}")
