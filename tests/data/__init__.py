from pathlib import Path

DATA_FOLDER = Path(__file__).resolve().parent

DESCRIPTIONS_FOLDER = DATA_FOLDER / "describe_data"
AUTH_DATA = DATA_FOLDER / "auth_data"
CUSTOM_PROJECT = DATA_FOLDER / "project_no_cognite_modules"
PROJECT_WITH_DUPLICATES = DATA_FOLDER / "project_with_duplicates"
PYTEST_PROJECT = DATA_FOLDER / "project_for_test"
LOAD_DATA = DATA_FOLDER / "load_data"
RUN_DATA = DATA_FOLDER / "run_data"
TRANSFORMATION_CLI = DATA_FOLDER / "transformation_cli"
PROJECT_WITH_BAD_MODULES = DATA_FOLDER / "project_with_bad_modules"
BUILD_CORE_MODEL = DATA_FOLDER / "build_core_model"
BUILD_GROUP_WITH_UNKNOWN_ACL = DATA_FOLDER / "build_group_with_unknown_acl"

__all__ = [
    "DATA_FOLDER",
    "DESCRIPTIONS_FOLDER",
    "AUTH_DATA",
    "CUSTOM_PROJECT",
    "PROJECT_WITH_DUPLICATES",
    "PYTEST_PROJECT",
    "LOAD_DATA",
    "RUN_DATA",
    "TRANSFORMATION_CLI",
    "PROJECT_WITH_BAD_MODULES",
    "BUILD_CORE_MODEL",
    "BUILD_GROUP_WITH_UNKNOWN_ACL",
]
