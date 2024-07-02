from typing import Literal

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError

from ._base import ToolkitCommand


class CollectCommand(ToolkitCommand):
    def execute(self, action: Literal["opt-in", "opt-out"]) -> None:
        if action == "opt-in":
            self.tracker.enable()
        elif action == "opt-out":
            self.tracker.disable()
        else:
            raise ToolkitValueError(f"Invalid action: {action}. Must be 'opt-in' or 'opt-out'.")
