from __future__ import annotations

from collections.abc import Callable
from enum import Enum
from pathlib import Path

from rich import print

from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.tk_warnings import NamingConventionWarning, ResourceMissingIdentifier, ToolkitWarning

from ._utils import resource_folder_from_path


class Resource(Enum):
    AUTH = "auth"
    DATA_SETS = "data_sets"
    DATA_MODELS = "data_models"
    RAW = "raw"
    WORKFLOWS = "workflows"
    TRANSFORMATIONS = "transformations"
    EXTRACTION_PIPELINES = "extraction_pipelines"
    TIMESERIES = "timeseries"
    FILES = "files"
    FUNCTIONS = "functions"
    OTHER = "other"

    @classmethod
    def _missing_(cls, value: object) -> Resource:
        """Returns Resource.OTHER for any other value."""
        assert isinstance(value, str)
        return cls.OTHER


def _extract_ext_id_dm_spaces(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        raise ToolkitYAMLFormatError(f"Multiple spaces in one file {filepath_src} is not supported.")
    elif isinstance(parsed, dict):
        ext_id = parsed.get("space")
    else:
        raise ToolkitYAMLFormatError(f"Space file {filepath_src} has invalid dataformat.")
    return ext_id, "space"


def _extract_ext_id_dm_nodes(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        raise ToolkitYAMLFormatError(f"Nodes YAML must be an object file {filepath_src} is not supported.")
    try:
        ext_ids = {source["source"]["externalId"] for node in parsed["nodes"] for source in node["sources"]}
    except KeyError:
        raise ToolkitYAMLFormatError(f"Node file {filepath_src} has invalid dataformat.")
    if len(ext_ids) != 1:
        raise ToolkitYAMLFormatError(f"All nodes in {filepath_src} must have the same view.")
    return ext_ids.pop(), "view.externalId"


def _extract_ext_id_auth(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        raise ToolkitYAMLFormatError(f"Multiple Groups in one file {filepath_src} is not supported.")
    return parsed.get("name"), "name"


def _extract_ext_id_function_schedules(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        return "", "multiple"
    elif isinstance(parsed, dict):
        ext_id = parsed.get("functionExternalId") or parsed.get("function_external_id")
        return ext_id, "functionExternalId"


def _extract_ext_id_functions(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        return "", "multiple"
    elif isinstance(parsed, dict):
        ext_id = parsed.get("externalId") or parsed.get("external_id")
        return ext_id, "externalId"


def _extract_ext_id_raw(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        return "", "multiple"
    elif isinstance(parsed, dict):
        ext_id, ext_id_type = parsed.get("dbName"), "dbName"
        if "tableName" in parsed:
            ext_id = f"{ext_id}.{parsed.get('tableName')}"
            ext_id_type = "dbName and tableName"
        return ext_id, ext_id_type
    else:
        raise ToolkitYAMLFormatError(f"Raw file {filepath_src} has invalid dataformat.")


def _extract_ext_id_workflows(parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, dict):
        if "version" in filepath_src.stem.lower():
            ext_id = parsed.get("workflowExternalId")
            ext_id_type = "workflowExternalId"
        else:
            ext_id = parsed.get("externalId") or parsed.get("external_id")
            ext_id_type = "externalId"
        return ext_id, ext_id_type
    else:
        raise ToolkitYAMLFormatError(f"Multiple Workflows in one file ({filepath_src}) is not supported.")


def _extract_ext_id_other(resource: Resource, parsed: dict | list, filepath_src: Path) -> tuple[str | None, str]:
    if isinstance(parsed, list):
        raise ToolkitYAMLFormatError(f"Multiple {resource} in one file {filepath_src} is not supported.")

    ext_id = parsed.get("externalId") or parsed.get("external_id")
    return ext_id, "externalId"


def _get_ext_id_and_type_from_parsed_yaml(
    resource: Resource, parsed: dict | list, filepath_src: Path
) -> tuple[str | None, str]:
    args: tuple[dict | list, Path] = (parsed, filepath_src)
    if resource is Resource.DATA_MODELS and ".space." in filepath_src.name:
        return _extract_ext_id_dm_spaces(*args)

    elif resource is Resource.DATA_MODELS and ".node." in filepath_src.name:
        return _extract_ext_id_dm_nodes(*args)

    elif resource is Resource.AUTH:
        return _extract_ext_id_auth(*args)

    elif resource in (Resource.DATA_SETS, Resource.TIMESERIES, Resource.FILES) and isinstance(parsed, list):
        return "", "multiple"

    elif resource is Resource.FUNCTIONS and "schedule" in filepath_src.stem:
        return _extract_ext_id_function_schedules(*args)

    elif resource is Resource.FUNCTIONS:
        return _extract_ext_id_functions(*args)

    elif resource is Resource.RAW:
        return _extract_ext_id_raw(*args)

    elif resource is Resource.WORKFLOWS:
        return _extract_ext_id_workflows(*args)

    else:
        return _extract_ext_id_other(resource, *args)


class YAMLSemantic:
    """
    Class to check the semantic correctness of a yaml file

    Args:
        warn (Callable[[ToolkitWarning], None]): A function to call when a warning is raised.
        verbose (bool): Turn on verbose mode.
    """

    def __init__(self, warn: Callable[[ToolkitWarning], None], verbose: bool = False) -> None:
        self.warn = warn
        self.verbose = verbose

    def check(self, parsed: dict | list, filepath_src: Path, filepath_build: Path) -> None:
        """Check the yaml file for semantic errors

        Args:
            parsed (dict | list): the loaded yaml file
            filepath_src (Path): the path to the yaml file
            filepath_build: (Path): No description


        Returns:
            None: File is semantically acceptable if no exceptions are raised.
        """
        if parsed is None or filepath_src is None or filepath_build is None:
            raise ToolkitYAMLFormatError

        resource_str = resource_folder_from_path(filepath_src)
        resource = Resource(resource_str)
        ext_id, ext_id_type = _get_ext_id_and_type_from_parsed_yaml(resource, parsed, filepath_src)

        if ext_id is None:
            self.warn(
                ResourceMissingIdentifier(
                    filepath=filepath_src, resource=filepath_build.parent.name, ext_id_type=ext_id_type
                )
            )
            raise ToolkitYAMLFormatError

        self._check_yaml_semantics(parsed, resource, filepath_src, ext_id, ext_id_type)

    def _check_yaml_semantics_auth(self, ext_id: str, filepath_src: Path) -> None:
        parts = ext_id.split("_")
        if len(parts) < 2:
            if ext_id == "applications-configuration":
                if self.verbose:
                    print(
                        "      [bold green]INFO:[/] the group applications-configuration does not follow the "
                        "recommended '_' based namespacing because Infield expects this specific name."
                    )
            else:
                self.warn(
                    NamingConventionWarning(
                        filepath_src, "auth", "name", ext_id, "without the recommended '_' based namespacing"
                    )
                )
        elif parts[0] != "gp":
            self.warn(
                NamingConventionWarning(
                    filepath_src, "auth", "name", ext_id, "without the recommended 'gp_' based prefix"
                )
            )

    def _check_yaml_semantics_transformations_schedules(self, ext_id: str, filepath_src: Path) -> None:
        parts = ext_id.split("_")
        if len(parts) < 2:
            self.warn(
                NamingConventionWarning(
                    filepath_src,
                    "transformation",
                    "externalId",
                    ext_id,
                    "without the recommended '_' based namespacing",
                )
            )
        elif parts[0] != "tr":
            self.warn(
                NamingConventionWarning(
                    filepath_src, "transformation", "externalId", ext_id, "without the recommended 'tr_' based prefix"
                )
            )

    def _check_yaml_semantics_dm_spaces(self, ext_id: str, filepath_src: Path) -> None:
        parts = ext_id.split("_")
        if len(parts) < 2:
            self.warn(
                NamingConventionWarning(
                    filepath_src, "space", "space", ext_id, "without the recommended '_' based namespacing"
                )
            )
        elif parts[0] != "sp":
            if ext_id == "cognite_app_data" or ext_id == "APM_SourceData" or ext_id == "APM_Config":
                if self.verbose:
                    print(
                        f"      [bold green]INFO:[/] the space {ext_id} does not follow the recommended '_' based "
                        "namespacing because Infield expects this specific name."
                    )
            else:
                self.warn(
                    NamingConventionWarning(
                        filepath_src, "space", "space", ext_id, "without the recommended 'sp_' based prefix"
                    )
                )

    def _check_yaml_semantics_extpipes(self, ext_id: str, filepath_src: Path) -> None:
        parts = ext_id.split("_")
        if len(parts) < 2:
            self.warn(
                NamingConventionWarning(
                    filepath_src,
                    "extraction pipeline",
                    "externalId",
                    ext_id,
                    "without the recommended '_' based namespacing",
                )
            )
        elif parts[0] != "ep":
            self.warn(
                NamingConventionWarning(
                    filepath_src,
                    "extraction pipeline",
                    "externalId",
                    ext_id,
                    "without the recommended 'ep_' based prefix",
                )
            )

    def _check_yaml_semantics_basic_resources(
        self, parsed: list | dict, resource: Resource, ext_id_type: str, ext_id: str, filepath_src: Path
    ) -> None:
        if not isinstance(parsed, list):
            parsed = [parsed]
        for ds in parsed:
            ext_id = ds.get("externalId") or ds.get("external_id")
            if ext_id is None:
                self.warn(
                    ResourceMissingIdentifier(filepath=filepath_src, resource=resource.value, ext_id_type=ext_id_type)
                )
                raise ToolkitYAMLFormatError
            parts = ext_id.split("_")
            # We don't want to throw a warning on entities that should not be governed by the tool
            # in production (i.e. fileseries, files, and other "real" data)
            if resource is Resource.DATA_SETS and len(parts) < 2:
                self.warn(
                    NamingConventionWarning(
                        filepath_src, "data set", "externalId", ext_id, "without the recommended '_' based namespacing"
                    )
                )

    def _check_yaml_semantics(
        self, parsed: list | dict, resource: Resource, filepath_src: Path, ext_id: str, ext_id_type: str
    ) -> None:
        args: tuple[str, Path] = ext_id, filepath_src
        if resource is Resource.AUTH:
            self._check_yaml_semantics_auth(*args)

        elif resource is Resource.TRANSFORMATIONS and not filepath_src.stem.endswith("schedule"):
            self._check_yaml_semantics_transformations_schedules(*args)

        elif resource is Resource.DATA_MODELS and ext_id_type == "space":
            self._check_yaml_semantics_dm_spaces(*args)

        elif resource is Resource.EXTRACTION_PIPELINES:
            self._check_yaml_semantics_extpipes(*args)

        elif resource in (Resource.DATA_SETS, Resource.TIMESERIES, Resource.FILES):
            self._check_yaml_semantics_basic_resources(parsed, resource, ext_id_type, *args)
