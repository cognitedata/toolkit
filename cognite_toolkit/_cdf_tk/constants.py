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
# Which suffixes to exclude when we create indexed files (i.e., they are bundled with their main config file)
EXCL_INDEX_SUFFIX = frozenset([".sql", ".csv", ".parquet"])
# Files to search for variables.
SEARCH_VARIABLES_SUFFIX = frozenset([".yaml", "yml", ".sql", ".csv"])
# Which suffixes to process for template variable replacement
PROC_TMPL_VARS_SUFFIX = frozenset([".yaml", ".yml", ".sql", ".csv", ".parquet", ".json", ".txt", ".md", ".html", ".py"])

ROOT_PATH = Path(__file__).parent.parent
COGNITE_MODULES_PATH = ROOT_PATH / COGNITE_MODULES
