#!/usr/bin/env python
import argparse
import logging
import sys
import time
from pathlib import Path

from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

root_folder = rf"{Path(Path(__file__).parent.absolute().parent)}"

sys.path.append(root_folder)

from dotenv import load_dotenv  # noqa: E402

log = logging.getLogger(__name__)


def run() -> None:
    print("Doing post-processing activities for demo project...")
    client = EnvironmentVariables.create_from_environment().get_client()
    try:
        print("Running tr_asset_oid_workmate_asset_hierarchy_example...")
        client.transformations.run(transformation_external_id="tr_asset_oid_workmate_asset_hierarchy_example")
        print("Running tr_workorder_oid_workmate_infield_sync_workorders_to_apm_activities...")
        client.transformations.run(
            transformation_external_id="tr_workorder_oid_workmate_infield_sync_workorders_to_apm_activities"
        )
        # Wait until assets are in the hierarchy
        time.sleep(10.0)
        print("Running tr_asset_oid_workmate_infield_sync_assets_from_hierarchy_to_apm...")
        client.transformations.run(
            transformation_external_id="tr_asset_oid_workmate_infield_sync_assets_from_hierarchy_to_apm"
        )
    except Exception as e:
        log.error(f"Failed to run post-processing activities for demo project:\n{e}")
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(epilog="Further functionality to be added")
    parser.add_argument(
        "--override_env",
        action="store_true",
        default=False,
        help="Override current environment variables with values from .env file",
    )
    args, unknown_args = parser.parse_known_args()
    # This is a convenience object that has a CDF client (.client) and
    # allows access to environment variables (.environ) using consistent
    # naming scheme that is also aligned with recommendations externally.
    if args.override_env:
        print("WARNING!!! Overriding environment variables with values from .env file...")
    if Path("../.env").is_file():
        load_dotenv("../.env", override=args.override_env)
    else:
        load_dotenv(".env", override=args.override_env)
    run()
