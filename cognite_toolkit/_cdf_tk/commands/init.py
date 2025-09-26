from rich import print
from rich.panel import Panel

from ._base import ToolkitCommand


class InitCommand(ToolkitCommand):
    def execute(self) -> None:
        print(Panel("This command is deprecated. Use 'cdf modules init' instead."))
