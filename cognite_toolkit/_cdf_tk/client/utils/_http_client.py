import sys
from typing import cast

from cognite.client._http_client import _RetryTracker

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ToolkitRetryTracker(_RetryTracker):
    def copy(self) -> Self:
        new_tracker = ToolkitRetryTracker(self.config)
        new_tracker.read = self.read
        new_tracker.connect = self.connect
        new_tracker.status = self.status
        return cast(Self, new_tracker)
