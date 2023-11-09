#!/usr/bin/env python
import argparse
import logging
import pathlib
import sys
import time

root_folder = rf"{pathlib.Path(pathlib.Path(__file__).parent.absolute().parent)}"

sys.path.append(root_folder)

from dotenv import load_dotenv  # noqa: E402

from cdf_project_template.utils import CDFToolConfig  # noqa: E402

log = logging.getLogger(__name__)


def run() -> None:
    print("Doing post-processing activities for demo project...")
    ToolGlobals = CDFToolConfig(client_name="cdf-project-templates")
    try:
        print("Running apm_simple-load-asset-hierarchy...")
        ToolGlobals.client.transformations.run(transformation_external_id="apm_simple-load-asset-hierarchy")
        print("Running sync_workorders_to_apm_activities...")
        ToolGlobals.client.transformations.run(transformation_external_id="sync_workorders_to_apm_activities")
        # Wait until assets are in the hierarchy
        time.sleep(10.0)
        print("Running sync_assets_from_hierarchy_to_apm...")
        ToolGlobals.client.transformations.run(transformation_external_id="sync_assets_from_hierarchy_to_apm")
        # Wait until assets are in data models
        time.sleep(10.0)
        print("Running sync_asset_parents_from_hierarchy_to_apm...")
        ToolGlobals.client.transformations.run(transformation_external_id="sync_asset_parents_from_hierarchy_to_apm")
    except Exception as e:
        log.error(f"Failed to run post-processing activities for demo project:\n{e}")
        exit(1)


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
    load_dotenv("../.env", override=args.override_env)
    run()
