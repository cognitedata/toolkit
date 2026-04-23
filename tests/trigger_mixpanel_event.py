"""This script is for triggering a Mixpanel event. It is used for testing purposes only."""

import sys

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.data_classes import (
    TrackingEvent,
)
from cognite_toolkit._cdf_tk.tracker import Tracker
from tests.auth_utils import get_toolkit_client
from tests.constants import REPO_ROOT, chdir


def track_test_command() -> None:
    sys.argv = ["commandTest", "pos0", "pos1", "--opt1", "opt1", "--opt2", "opt2", "--flag"]

    with chdir(REPO_ROOT):
        # To ensure that cdf.toml is loaded correctly
        _ = CDFToml.load()
        tracker = Tracker()

        is_sent = tracker.track(TrackingEvent(event_name="test"), get_toolkit_client(".env"))
        if is_sent:
            print("Event sent")
        else:
            print("Event not sent")


if __name__ == "__main__":
    track_test_command()
