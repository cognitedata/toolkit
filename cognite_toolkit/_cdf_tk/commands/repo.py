from pathlib import Path

from ._base import ToolkitCommand


class RepoCommand(ToolkitCommand):
    def __init__(self, print_warning: bool = True, skip_tracking: bool = False, silent: bool = False) -> None:
        super().__init__(print_warning=print_warning, skip_tracking=skip_tracking, silent=silent)

    def init(self, cwd: Path) -> None:
        print("Initializing git repository...")
