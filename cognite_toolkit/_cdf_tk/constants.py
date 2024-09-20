import re
from pathlib import Path

from typing_extensions import Literal, TypeAlias

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

BUILTIN_MODULES = "_builtin_modules"
COGNITE_MODULES = "cognite_modules"
CUSTOM_MODULES = "custom_modules"
MODULES = "modules"
REPO_FILES_DIR = "_repo_files"

ROOT_MODULES = [MODULES, CUSTOM_MODULES, COGNITE_MODULES]
MODULE_PATH_SEP = "/"

MIN_TIMESTAMP_MS = -2208988800000  # 1900-01-01 00:00:00.000
MAX_TIMESTAMP_MS = 4102444799999  # 2099-12-31 23:59:59.999
HAS_DATA_FILTER_LIMIT = 10

DEFAULT_ENV = "dev"
# Add any other files below that should be included in a build
EXCL_FILES = ["README.md", DEFAULT_CONFIG_FILE]
# Files to search for variables.
SEARCH_VARIABLES_SUFFIX = frozenset([".yaml", "yml", ".sql", ".csv"])
# Which files to process for template variable replacement
TEMPLATE_VARS_FILE_SUFFIXES = frozenset([".yaml", ".yml", ".sql", ".json", ".csv", ".txt", ".md", ".html", ".py"])
ROOT_PATH = Path(__file__).parent.parent
COGNITE_MODULES_PATH = ROOT_PATH / COGNITE_MODULES
MODULES_PATH = ROOT_PATH / MODULES
BUILTIN_MODULES_PATH = ROOT_PATH / BUILTIN_MODULES
SUPPORT_MODULE_UPGRADE_FROM_VERSION = "0.2.20"
# This is used in the build directory to keep track of order and flatten the
# module directory structure with accounting for duplicated names.
INDEX_PATTERN = re.compile("^[0-9]+\\.")

# This is a regular expression that matches any non-word character or underscore
# It is used to clean the feature flag names.
_CLEAN_PATTERN = re.compile(r"[\W_]+")

EnvType: TypeAlias = Literal["dev", "test", "staging", "qa", "prod"]


def clean_name(name: str) -> str:
    """Cleans the name by removing any non-word characters or underscores."""
    return _CLEAN_PATTERN.sub("", name).casefold()


class URL:
    configure_access = "https://docs.cognite.com/cdf/deploy/cdf_deploy/cdf_deploy_access_management"
    auth_toolkit = "https://docs.cognite.com/cdf/deploy/cdf_toolkit/guides/configure_deploy_modules#configure-the-cdf-toolkit-authentication"
    docs = "https://docs.cognite.com/"
    configs = "https://docs.cognite.com/cdf/deploy/cdf_toolkit/references/configs"
