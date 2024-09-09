"""This script is for triggering a Mixpanel event. It is used for testing purposes only."""

import sys

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.tk_warnings import WarningList
from cognite_toolkit._cdf_tk.tracker import Tracker
from tests.constants import REPO_ROOT, chdir


def main() -> None:
    sys.argv = ["commandTest", "pos0", "pos1", "--opt1", "opt1", "--opt2", "opt2", "--flag"]

    with chdir(REPO_ROOT):
        # To ensure that cdf.toml is loaded correctly
        _ = CDFToml.load()
        tracker = Tracker(" ".join(sys.argv))
        tracker.track_command(WarningList([]), "Success", "test")
        print("Event sent")


if __name__ == "__main__":
    main()
