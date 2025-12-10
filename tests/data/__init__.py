from pathlib import Path

DATA_FOLDER = Path(__file__).resolve().parent
CALC_HASH_DATA = DATA_FOLDER / "calc_hash_data"
AUTH_DATA = DATA_FOLDER / "auth_data"
PROJECT_NO_COGNITE_MODULES = DATA_FOLDER / "project_no_cognite_modules"
PROJECT_WITH_DUPLICATES = DATA_FOLDER / "project_with_duplicates"
PROJECT_FOR_TEST = DATA_FOLDER / "project_for_test"
LOAD_DATA = DATA_FOLDER / "load_data"
RUN_DATA = DATA_FOLDER / "run_data"
TRANSFORMATION_CLI = DATA_FOLDER / "transformation_cli"
PROJECT_WITH_BAD_MODULES = DATA_FOLDER / "project_with_bad_modules"
NAUGHTY_PROJECT = DATA_FOLDER / "naughty_project"
EXTERNAL_PACKAGE = DATA_FOLDER / "external_package"

BUILD_GROUP_WITH_UNKNOWN_ACL = DATA_FOLDER / "build_group_with_unknown_acl"
COMPLETE_ORG = DATA_FOLDER / "complete_org"
COMPLETE_ORG_ALPHA_FLAGS = DATA_FOLDER / "complete_org_alpha_flags"
COMPLETE_ORG_ONLY_IDENTIFIER = DATA_FOLDER / "complete_org_only_identifier"
BUILDABLE_PACKAGE = DATA_FOLDER / "buildable_package"
CDF_TOML_DATA = DATA_FOLDER / "cdf_toml_data"
STRONGLY_COUPLED_MODEL = DATA_FOLDER / "strongly_coupled_model"

CORE_NO_3D_YAML = DATA_FOLDER / "cdf" / "core_no_3d.yaml"
CORE_CONTAINERS_NO_3D_YAML = DATA_FOLDER / "cdf" / "core_containers_no_3d.yaml"
EXTRACTOR_VIEWS_YAML = DATA_FOLDER / "cdf" / "extractor_views.yaml"
INFIELD_CDM_LOCATION_CONFIG_VIEW_YAML = DATA_FOLDER / "cdf" / "infield_cdm_location_config_view.yaml"
INFIELD_CDM_LOCATION_CONFIG_CONTAINER_YAML = DATA_FOLDER / "cdf" / "infield_cdm_location_config_container.yaml"
THREE_D_MODEL = DATA_FOLDER / "3d_model"
THREE_D_He2_FBX_ZIP = THREE_D_MODEL / "he2.zip"

__all__ = [
    "AUTH_DATA",
    "BUILDABLE_PACKAGE",
    "BUILD_GROUP_WITH_UNKNOWN_ACL",
    "CDF_TOML_DATA",
    "COMPLETE_ORG",
    "EXTERNAL_PACKAGE",
    "LOAD_DATA",
    "PROJECT_FOR_TEST",
    "PROJECT_NO_COGNITE_MODULES",
    "PROJECT_WITH_BAD_MODULES",
    "PROJECT_WITH_DUPLICATES",
    "RUN_DATA",
    "STRONGLY_COUPLED_MODEL",
    "TRANSFORMATION_CLI",
]
