from __future__ import annotations

import sys
from pathlib import Path

from cognite.client import CogniteClient
from cognite.client.data_classes import Row
from cognite.extractorutils.uploader import RawUploadQueue

sys.path.append(str(Path(__file__).parent))

from config import ContextConfig


def write_mapping_to_raw(
    cognite_client: CogniteClient,
    config: ContextConfig,
    raw_uploader: RawUploadQueue,
    good_matches: list[Row],
    bad_matches: list[Row],
) -> None:
    """
    Write matching results to RAW DB

    Args:
        cognite_client: Instance of CogniteClient
        config: Instance of ContextConfig
        raw_uploader : Instance of RawUploadQueue
        good_matches: list of good matches
        bad_matches: list of bad matches
    """

    print(f"INFO: Clean up BAD table: {config.rawdb}/{config.raw_table_bad} before writing new status")
    try:
        cognite_client.raw.tables.delete(config.rawdb, [config.raw_table_bad])
    except Exception:
        pass  # no table to delete

    # if reset mapping, clean up good matches in table
    if config.run_all and not config.debug:
        print(
            f"INFO: ResetMapping - Cleaning up GOOD table: {config.rawdb}/{config.raw_table_good} "
            "before writing new status"
        )
        try:
            cognite_client.raw.tables.delete(config.rawdb, [config.raw_table_good])
        except Exception:
            pass  # no table to delete

    for match in good_matches:
        # Add to RAW upload queue
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_good, Row(match["ts_ext_id"], match))
    print(f"INFO: Added {len(good_matches)} to {config.rawdb}/{config.raw_table_good}")

    for not_match in bad_matches:
        # Add to RAW upload queue
        raw_uploader.add_to_upload_queue(config.rawdb, config.raw_table_bad, Row(not_match["ts_ext_id"], not_match))
    print(f"INFO: Added {len(bad_matches)} to {config.rawdb}/{config.raw_table_bad}")

    # Upload any remaining RAW cols in queue
    raw_uploader.upload()
