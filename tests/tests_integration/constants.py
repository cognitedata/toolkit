import platform
import random
import sys

# This is needed as we run tests for two different versions of Python in parallel.
# The platform.system() is not used, but is here in case we start testing on Windows as well.
# The random number is to avoid conflicts when running tests in parallel (for example, two PRs).
RUN_UNIQUE_ID = f"{platform.system()}_{sys.version_info.major}_{sys.version_info.minor}_{random.randint(0, 10)!s}"
