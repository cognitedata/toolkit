from typing import Literal

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError

from ._base import ToolkitCommand


class CollectCommand(ToolkitCommand):
    def execute(self, action: Literal["opt-in", "opt-out"]) -> None:
        if action == "opt-in":
            self.tracker.enable()
            print("You have successfully opted in to data collection.")
        elif action == "opt-out":
            self.tracker.disable()
            print("You have successfully opted out of data collection.")
        else:
            raise ToolkitValueError(f"Invalid action: {action}. Must be 'opt-in' or 'opt-out'.")
