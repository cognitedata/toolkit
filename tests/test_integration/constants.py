import platform
import sys

# This is needed as we run tests for two different versions of Python in parallel.
# The platform.system() is not used, but is here in case we start testing on Windows as well.
RUN_UNIQUE_ID = f"{platform.system()}-{sys.version_info.major}-{sys.version_info.minor}"
