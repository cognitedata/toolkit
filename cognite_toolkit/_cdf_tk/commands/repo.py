from pathlib import Path

from ._base import ToolkitCommand


class RepoCommand(ToolkitCommand):
    def init(self, cwd: Path) -> None:
        print("Initializing git repository...")
