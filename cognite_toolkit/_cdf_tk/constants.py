import re
from pathlib import Path

try:
    from pyodide.ffi import IN_BROWSER  # type: ignore [import-not-found]
except ModuleNotFoundError:
    IN_BROWSER = False
# This is the default config located locally in each module.
# The environment file:


_RUNNING_IN_BROWSER = IN_BROWSER
# This is the default config located locally in each module.
DEFAULT_CONFIG_FILE = "default.config.yaml"
# The environment file:
BUILD_ENVIRONMENT_FILE = "_build_environment.yaml"
# The local config file:
CONFIG_FILE_SUFFIX = "config.yaml"
# The global config file
GLOBAL_CONFIG_FILE = "global.yaml"

COGNITE_MODULES = "cognite_modules"
CUSTOM_MODULES = "custom_modules"
ALT_CUSTOM_MODULES = "modules"

ROOT_MODULES = [COGNITE_MODULES, CUSTOM_MODULES, ALT_CUSTOM_MODULES]
MODULE_PATH_SEP = "/"

# Add any other files below that should be included in a build
EXCL_FILES = ["README.md", DEFAULT_CONFIG_FILE]
# Files to search for variables.
SEARCH_VARIABLES_SUFFIX = frozenset([".yaml", "yml", ".sql", ".csv"])
# Which files to process for template variable replacement
TEMPLATE_VARS_FILE_SUFFIXES = frozenset(
    [".yaml", ".yml", ".sql", ".csv", ".parquet", ".json", ".txt", ".md", ".html", ".py"]
)
ROOT_PATH = Path(__file__).parent.parent
COGNITE_MODULES_PATH = ROOT_PATH / COGNITE_MODULES

SUPPORT_MODULE_UPGRADE_FROM_VERSION = "0.1.0"
# This is used in the build directory to keep track of order and flatten the
# module directory structure with accounting for duplicated names.
INDEX_PATTERN = re.compile("^[0-9]+\\.")


class URL:
    configure_access = "https://docs.cognite.com/cdf/deploy/cdf_deploy/cdf_deploy_access_management"
    auth_toolkit = "https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/configure_deploy_modules#configure-the-cdf-toolkit-authentication"
    docs = "https://docs.cognite.com/"
    configs = "https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/configs"
