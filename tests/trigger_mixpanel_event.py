"""This script is for triggering a Mixpanel event. It is used for testing purposes only."""

import sys

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.data_classes import (
    CommandTracking,
    DeploymentTracking,
    TrackingEvent,
)
from cognite_toolkit._cdf_tk.data_classes._tracking_info import (
    BuildTracking,
    DataTracking,
    ResourceBuildStat,
    ResourceDeploymentStat,
)
from cognite_toolkit._cdf_tk.dataio.logger import ItemsResult
from cognite_toolkit._cdf_tk.tracker import Tracker
from tests.auth_utils import get_toolkit_client
from tests.constants import REPO_ROOT, chdir


def _mock_events() -> list[TrackingEvent]:
    """Build one mock event per available TrackingEvent subclass."""
    command_event = CommandTracking(
        event_name="test",
        installed_module_ids={"module_a", "module_b"},
        installed_package_ids={"package_a"},
        downloaded_library_ids={"library_a"},
        downloaded_package_ids={"package_b"},
        downloaded_module_ids={"module_c"},
        warning_total_count=3,
        result="success",
        error=None,
        subcommands=["build"],
        alpha_flags=["experimental-feature"],
        plugins=["plugin_a"],
    )

    data_event = DataTracking.from_item_results(
        event_name="DownloadResult",
        data_type="timeseries",
        item_results=[
            ItemsResult(status="success", count=42, severity=0),
            ItemsResult(status="failure", count=2, severity=3),
            ItemsResult(status="skipped", count=1, severity=0),
            ItemsResult(status="success-with-warning", count=5, severity=1),
        ],
    )

    deployment_event = DeploymentTracking(
        is_dry_run=False,
        operation="deploy",
        resource_types=["timeseries", "assets"],
        resource_stats=[
            ResourceDeploymentStat(
                resource_name="timeseries",
                created=10,
                updated=2,
                deleted=0,
                unchanged=5,
                skipped=1,
                total=18,
            ),
            ResourceDeploymentStat(
                resource_name="assets",
                created=3,
                updated=1,
                deleted=0,
                unchanged=7,
                skipped=0,
                total=11,
            ),
        ],
        total_created=13,
        total_updated=3,
        total_deleted=0,
        total_unchanged=12,
        total_skipped=1,
        total_resources=29,
        resource_type_count=2,
    )

    build_event = BuildTracking(
        build_duration_ms=1234,
        resource_types=["timeseries", "assets"],
        resource_stats=[
            ResourceBuildStat(
                resource_folder="timeseries",
                kind="TimeSeries",
                built_count=10,
                dependency_total=2,
                syntax_error_count=0,
            ),
            ResourceBuildStat(
                resource_folder="assets",
                kind="Asset",
                built_count=5,
                dependency_total=1,
                syntax_error_count=0,
            ),
        ],
        insight_codes=["INSIGHT_001"],
        dependency_total=3,
        dependency_average=1.5,
        built_resource_total=15,
        module_count=4,
        insight_total_count=1,
        yaml_line_count=250,
    )

    return [command_event, data_event, deployment_event, build_event]


def track_test_command() -> None:
    sys.argv = ["commandTest", "pos0", "pos1", "--opt1", "opt1", "--opt2", "opt2", "--flag"]

    with chdir(REPO_ROOT):
        # To ensure that cdf.toml is loaded correctly
        _ = CDFToml.load()
        tracker = Tracker()
        client = get_toolkit_client(".env")

        for event in _mock_events():
            is_sent = tracker.track(event, client)
            label = type(event).__name__
            if is_sent:
                print(f"{label} event sent")
            else:
                print(f"{label} event not sent")


if __name__ == "__main__":
    track_test_command()
