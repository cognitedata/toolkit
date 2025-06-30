import platform
import random
import sys

# This is needed as we run tests for two different versions of Python in parallel.
# The platform.system() is not used, but is here in case we start testing on Windows as well.
# The random number is to avoid conflicts when running tests in parallel (for example, two PRs).
RUN_UNIQUE_ID = f"{platform.system()}_{sys.version_info.major}_{sys.version_info.minor}_{random.randint(0, 10)!s}"

# These constants are used to set up a raw database and tables with transformations and datasets
# to populate the asset-centric resources in CDF.
ASSET_TABLE = "toolkit_aggregators_test_table_assets"
EVENT_TABLE = "toolkit_aggregators_test_table_events"
FILE_TABLE = "toolkit_aggregators_test_table_files"
TIMESERIES_TABLE = "toolkit_aggregators_test_table_time_series"
SEQUENCE_TABLE = "toolkit_aggregators_test_table_sequences"

ASSET_TRANSFORMATION = "toolkit_aggregators_test_asset_transformation"
EVENT_TRANSFORMATION = "toolkit_aggregators_test_event_transformation"
FILE_TRANSFORMATION = "toolkit_aggregators_test_file_transformation"
TIMESERIES_TRANSFORMATION = "toolkit_aggregators_test_timeseries_transformation"
SEQUENCE_TRANSFORMATION = "toolkit_aggregators_test_sequence_transformation"

ASSET_DATASET = "toolkit_aggregators_test_dataset_1"
EVENT_DATASET = "toolkit_aggregators_test_dataset_1"
FILE_DATASET = "toolkit_aggregators_test_dataset_2"
TIMESERIES_DATASET = "toolkit_aggregators_test_dataset_2"
SEQUENCE_DATASET = "toolkit_aggregators_test_dataset_2"

ASSET_COUNT = 6
EVENT_COUNT = 10
FILE_COUNT = 3
TIMESERIES_COUNT = 20
SEQUENCE_COUNT = 2
