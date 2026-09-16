from pathlib import Path
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client.resource_classes.function_app import FunctionAppRequest, FunctionAppResponse
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported
from cognite_toolkit._cdf_tk.resource_ios import FunctionAppIO, FunctionIO, get_crud
from cognite_toolkit._cdf_tk.yaml_classes import FunctionAppsYAML


def test_function_app_yaml_rejects_owner() -> None:
    with pytest.raises(ValidationError, match="remove this field"):
        FunctionAppsYAML.model_validate({"externalId": "app", "name": "App", "owner": "someone"})


def test_function_app_request_excludes_dataset_id() -> None:
    request = FunctionAppRequest(external_id="app", name="App", file_id=1, data_set_id=2)
    assert "dataSetId" not in request.dump()


def test_function_app_response_has_alpha_wire_status() -> None:
    response = FunctionAppResponse(id=1, created_time=2, external_id="app", name="App", file_id=3, status="ready")
    assert response.status == "ready"


def test_function_app_load_resource_normalizes_null_metadata(tmp_path: Path) -> None:
    resource_file = tmp_path / "app.FunctionApp.yaml"
    resource_file.write_text("externalId: app\nname: App\nmetadata: null\nsecrets:\n  password: secret\n")
    loader = FunctionAppIO.create_loader(MagicMock(), tmp_path.parent)

    loaded = loader.load_resource_file(resource_file)

    assert loaded[0]["metadata"]["cdf-toolkit-secret-hash"]
    assert list(loader.sensitive_strings(loader.load_resource(loaded[0]))) == ["secret"]


@pytest.mark.parametrize("external_id", ["../secret", "/tmp/secret", "nested/secret", "nested\\secret"])
def test_function_app_rejects_unsafe_code_paths(tmp_path: Path, external_id: str) -> None:
    resource_file = tmp_path / "app.FunctionApp.yaml"

    [failed] = FunctionAppIO.get_extra_files(resource_file, FunctionAppIO.get_id({"externalId": external_id}), {})

    assert failed.code == "SYNTAX-ERROR"
    assert "Invalid identifier" in failed.error


def test_function_app_rejects_symlinked_code_directory(tmp_path: Path) -> None:
    resource_file = tmp_path / "app.FunctionApp.yaml"
    outside = tmp_path.parent / "outside"
    outside.mkdir(exist_ok=True)
    (tmp_path / "app").symlink_to(outside, target_is_directory=True)

    [failed] = FunctionAppIO.get_extra_files(resource_file, FunctionAppIO.get_id({"externalId": "app"}), {})

    assert failed.code == "SYNTAX-ERROR"
    assert "path must remain inside" in failed.error


@pytest.mark.parametrize("method, argument", [("create", []), ("retrieve", []), ("delete", []), ("iterate", None)])
def test_function_app_deployment_stubs_are_typed(method: str, argument: list[object] | None) -> None:
    loader = FunctionAppIO.create_loader(MagicMock(), None)
    with pytest.raises(ToolkitNotSupported, match="alpha resource supports build only"):
        if argument is None:
            list(loader.iterate())
        else:
            getattr(loader, method)(argument)


def test_function_and_function_app_share_folder_without_misattributing_sidecars(tmp_path: Path) -> None:
    function_yaml = tmp_path / "classic.Function.yaml"
    app_yaml = tmp_path / "app.FunctionApp.yaml"
    function_sidecar = tmp_path / "classic.FileMetadata.yaml"
    app_sidecar = tmp_path / "app.FileMetadata.yaml"
    function_yaml.write_text("externalId: function\nname: Function\n")
    app_yaml.write_text("externalId: app\nname: App\n")
    function_sidecar.write_text("externalId: classic-code\nname: code\n")
    app_sidecar.write_text("externalId: app-code\nname: code\n")

    function = FunctionIO.create_loader(MagicMock(), tmp_path.parent)
    app = FunctionAppIO.create_loader(MagicMock(), tmp_path.parent)
    function.load_resource_file(function_yaml)
    app.load_resource_file(app_yaml)

    # Sidecars are only associated by the corresponding resource filename stem.
    assert function.filemetadata_path_by_external_id["function"] == function_sidecar
    assert app._code_bundle.filemetadata_path_by_external_id["app"] == app_sidecar
    assert get_crud("functions", "Function") is FunctionIO
    assert get_crud("functions", "FunctionApp") is FunctionAppIO


def test_function_app_is_available_to_alpha_deserialization_only() -> None:
    from cognite_toolkit._cdf_tk.resource_ios import CRUDS_BY_FOLDER_NAME, CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA

    assert FunctionAppIO in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA["functions"]
    assert (FunctionAppIO in CRUDS_BY_FOLDER_NAME["functions"]) is (
        FunctionAppIO
        not in __import__("cognite_toolkit._cdf_tk.resource_ios", fromlist=["_EXCLUDED_CRUDS"])._EXCLUDED_CRUDS
    )


def test_shared_function_sidecar_is_only_classified_once(tmp_path: Path) -> None:
    from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeployV2Command

    function_dir = tmp_path / "functions"
    function_dir.mkdir()
    (function_dir / "classic.Function.yaml").write_text("externalId: classic\nname: Classic\n")
    (function_dir / "app.FunctionApp.yaml").write_text("externalId: app\nname: App\n")
    sidecar = function_dir / "code.FileMetadata.yaml"
    sidecar.write_text("externalId: code\nname: code\n")

    read = DeployV2Command(print_warning=False, skip_tracking=True, silent=True).read_build_directory(tmp_path)

    assert read.resource_directories[0].extra_files == [sidecar]
