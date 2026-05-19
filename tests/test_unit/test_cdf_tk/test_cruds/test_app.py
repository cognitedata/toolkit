import io
import zipfile
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId, ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppResponse
from cognite_toolkit._cdf_tk.client.resource_classes.app_version import AppVersionRequest, AppVersionResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.app import AppIO, AppVersionIO


def _make_app_version_request(
    app_external_id: str = "my-app",
    version: str = "1.0.0",
    lifecycle_state: str = "PUBLISHED",
    alias: str | None = None,
    entrypoint: str = "index.html",
) -> AppVersionRequest:
    return AppVersionRequest(
        app_external_id=app_external_id,
        version=version,
        lifecycle_state=lifecycle_state,
        alias=alias,
        entrypoint=entrypoint,
    )


def _make_app_response(
    app_external_id: str = "my-app",
    version: str = "1.0.0",
    lifecycle_state: str = "PUBLISHED",
    alias: str | None = "ACTIVE",
) -> AppVersionResponse:
    return AppVersionResponse(
        app_external_id=app_external_id,
        version=version,
        lifecycle_state=lifecycle_state,
        alias=alias,
    )


def _write_zip(path: Path, filenames: list[str] | None = None) -> None:
    if filenames is None:
        filenames = ["index.html"]
    with zipfile.ZipFile(path, "w") as zf:
        for filename in filenames:
            zf.writestr(filename, b"content")



class TestAppIODumpResource:
    @pytest.mark.parametrize(
        "description, expected",
        [
            pytest.param(
                "A great app",
                {"externalId": "my-app", "name": "My App", "description": "A great app"},
                id="includes-description",
            ),
            pytest.param(
                None,
                {"externalId": "my-app", "name": "My App"},
                id="omits-none-description",
            ),
        ],
    )
    def test_dump_fields(self, description, expected):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, None)

        response = AppResponse(external_id="my-app", name="My App", description=description)
        assert loader.dump_resource(response) == expected

    def test_dump_prefers_local_name_and_description(self):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, None)

        response = AppResponse(external_id="my-app", name="Remote Name")
        local = {"name": "Local Name", "description": "Local desc"}
        dumped = loader.dump_resource(response, local=local)

        assert dumped["name"] == "Local Name"
        assert dumped["description"] == "Local desc"


class TestAppVersionIOGetId:
    def test_from_dict(self):
        assert AppVersionIO.get_id({"appExternalId": "my-app", "version": "1.0.0"}) == AppVersionId(
            app_external_id="my-app", version="1.0.0"
        )

    @pytest.mark.parametrize(
        "item",
        [
            {"version": "1.0.0"},
            {"appExternalId": "my-app"},
        ],
    )
    def test_from_dict_raises_when_field_missing(self, item: dict):
        with pytest.raises(KeyError):
            AppVersionIO.get_id(item)


class TestAppVersionIODependencies:
    def test_get_dependent_items_yields_app_io(self):
        item = {"appExternalId": "my-app", "version": "1.0.0"}
        deps = list(AppVersionIO.get_dependent_items(item))

        assert len(deps) == 1
        assert deps[0][0] is AppIO
        assert deps[0][1] == ExternalId(external_id="my-app")

    def test_get_dependent_items_returns_empty_without_app_external_id(self):
        deps = list(AppVersionIO.get_dependent_items({"version": "1.0.0"}))
        assert deps == []


class TestAppVersionIODeploy:
    @pytest.fixture
    def version_io_with_zip(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppVersionIO.create_loader(client, tmp_path)
            zip_path = tmp_path / "my-app.zip"
            _write_zip(zip_path)
            version_id = AppVersionId(app_external_id="my-app", version="1.0.0")
            loader.zip_path_by_version_id[version_id] = zip_path
            yield loader, client

    def test_deploy_uploads_zip(self, version_io_with_zip):
        loader, client = version_io_with_zip
        item = _make_app_version_request(lifecycle_state="DRAFT", alias=None)

        loader.create([item])

        client.tool.apps.create.assert_not_called()
        client.tool.apps.versions.upload.assert_called_once_with(
            external_id="my-app",
            version="1.0.0",
            entrypoint="index.html",
            zip_bytes=loader.zip_path_by_version_id[
                AppVersionId(app_external_id="my-app", version="1.0.0")
            ].read_bytes(),
        )

    @pytest.mark.parametrize(
        "alias, expected_alias_patch",
        [
            pytest.param("ACTIVE", {"set": "ACTIVE"}, id="sets-alias"),
            pytest.param(None, {"setNull": True}, id="clears-alias"),
        ],
    )
    def test_deploy_sets_lifecycle_and_alias(self, version_io_with_zip, alias, expected_alias_patch):
        loader, client = version_io_with_zip
        item = _make_app_version_request(lifecycle_state="PUBLISHED", alias=alias)

        loader.create([item])

        client.tool.apps.versions.update.assert_called_once_with(
            "my-app", "1.0.0", {"lifecycleState": {"set": "PUBLISHED"}, "alias": expected_alias_patch}
        )

    def test_deploy_raises_when_zip_missing(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppVersionIO.create_loader(client, tmp_path)
            version_id = AppVersionId(app_external_id="my-app", version="1.0.0")
            loader.zip_path_by_version_id[version_id] = tmp_path / "my-app.zip"  # registered but not on disk
            item = _make_app_version_request()
            with pytest.raises(ToolkitRequiredValueError, match="my-app"):
                loader.create([item])

    def test_deploy_returns_response_with_correct_fields(self, version_io_with_zip):
        loader, _client = version_io_with_zip
        item = _make_app_version_request(lifecycle_state="PUBLISHED", alias="ACTIVE")

        results = loader.create([item])

        assert len(results) == 1
        response = results[0]
        assert isinstance(response, AppVersionResponse)
        assert response.app_external_id == "my-app"
        assert response.version == "1.0.0"
        assert response.lifecycle_state == "PUBLISHED"
        assert response.alias == "ACTIVE"


class TestAppVersionIOLoadResourceFile:
    def test_registers_zip_path_for_valid_yaml(self, tmp_path: Path):
        apps_dir = tmp_path / "apps"
        apps_dir.mkdir()
        yaml_file = apps_dir / "my-app.AppVersion.yaml"
        yaml_file.write_text("appExternalId: my-app\nversion: 1.0.0\n")

        with monkeypatch_toolkit_client() as client:
            loader = AppVersionIO.create_loader(client, tmp_path)
            result = loader.load_resource_file(yaml_file)

        assert result == [{"appExternalId": "my-app", "version": "1.0.0"}]
        version_id = AppVersionId(app_external_id="my-app", version="1.0.0")
        assert version_id in loader.zip_path_by_version_id
        assert loader.zip_path_by_version_id[version_id] == apps_dir / "my-app.zip"

    def test_returns_empty_when_parent_not_apps(self, tmp_path: Path):
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        yaml_file = other_dir / "my-app.AppVersion.yaml"
        yaml_file.write_text("appExternalId: my-app\nversion: 1.0.0\n")

        with monkeypatch_toolkit_client() as client:
            loader = AppVersionIO.create_loader(client, tmp_path)
            result = loader.load_resource_file(yaml_file)

        assert result == []



class TestAppVersionIODumpResource:
    def test_dump_uses_app_external_id_key(self):
        with monkeypatch_toolkit_client() as client:
            loader = AppVersionIO.create_loader(client, None)

        response = AppVersionResponse(
            app_external_id="my-app",
            version="1.0.0",
            lifecycle_state="PUBLISHED",
            alias="ACTIVE",
        )
        dumped = loader.dump_resource(response)

        assert dumped["appExternalId"] == "my-app"
        assert dumped["version"] == "1.0.0"
        assert dumped["lifecycleState"] == "PUBLISHED"
        assert dumped["alias"] == "ACTIVE"

    def test_copies_source_path_from_local(self):
        with monkeypatch_toolkit_client() as client:
            loader = AppVersionIO.create_loader(client, None)

        response = AppVersionResponse(
            app_external_id="my-app",
            version="1.0.0",
            lifecycle_state="PUBLISHED",
            alias="ACTIVE",
        )
        local = {"sourcePath": "../../../../my-custom-app"}

        dumped = loader.dump_resource(response, local=local)

        assert dumped["sourcePath"] == "../../../../my-custom-app"


class TestAppVersionIOGetExtraFiles:
    def test_yields_zip_with_dist_contents(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        dist_dir = app_dir / "dist"
        dist_dir.mkdir(parents=True)
        (dist_dir / "index.html").write_text("<html></html>")
        (dist_dir / "bundle.js").write_text("console.log('hi')")
        (app_dir / "package.json").write_text("{}")
        (app_dir / "package-lock.json").write_text("{}")

        yaml_file = tmp_path / "my-app.AppVersion.yaml"
        yaml_file.write_text("")
        item = {"appExternalId": "my-app", "version": "1.0.0"}

        extras = list(
            AppVersionIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item)
        )

        assert len(extras) == 1
        extra = extras[0]
        assert extra.suffix == ".zip"
        assert extra.byte_content is not None
        with zipfile.ZipFile(io.BytesIO(extra.byte_content)) as zf:
            names = zf.namelist()
        assert any("index.html" in n for n in names)
        assert any("bundle.js" in n for n in names)
        assert ".cognite/package.json" in names
        assert ".cognite/package-lock.json" in names

    def test_falls_back_to_root_without_dist(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        app_dir.mkdir()
        (app_dir / "index.html").write_text("<html></html>")
        (app_dir / "package.json").write_text("{}")
        (app_dir / "package-lock.json").write_text("{}")

        yaml_file = tmp_path / "my-app.AppVersion.yaml"
        yaml_file.write_text("")
        item = {"appExternalId": "my-app", "version": "1.0.0"}

        extras = list(
            AppVersionIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item)
        )

        assert len(extras) == 1
        assert extras[0].suffix == ".zip"

    def test_fails_when_entrypoint_missing_from_root_and_dist(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        app_dir.mkdir()

        yaml_file = tmp_path / "my-app.AppVersion.yaml"
        yaml_file.write_text("")
        item = {"appExternalId": "my-app", "version": "1.0.0"}

        extras = list(
            AppVersionIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item)
        )

        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)
        assert "index.html" in extras[0].error

    def test_fails_with_build_hint_when_unbuilt_webapp(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        app_dir.mkdir()
        (app_dir / "src").mkdir()
        (app_dir / "package.json").write_text("{}")
        (app_dir / "package-lock.json").write_text("{}")
        (app_dir / "index.html").write_text("<html></html>")

        yaml_file = tmp_path / "my-app.AppVersion.yaml"
        yaml_file.write_text("")
        item = {"appExternalId": "my-app", "version": "1.0.0"}

        extras = list(
            AppVersionIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item)
        )

        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)
        assert "npm run build" in extras[0].error

    @pytest.mark.parametrize(
        "item",
        [
            pytest.param({"appExternalId": "my-app", "version": "1.0.0"}, id="default-path"),
            pytest.param({"appExternalId": "my-app", "version": "1.0.0", "sourcePath": "does-not-exist"}, id="source-path"),
        ],
    )
    def test_fails_when_app_dir_not_found(self, tmp_path: Path, item: dict):
        yaml_file = tmp_path / "my-app.AppVersion.yaml"
        yaml_file.write_text("")

        extras = list(
            AppVersionIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item)
        )

        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)

    def test_uses_source_path_field(self, tmp_path: Path):
        external_dir = tmp_path / "my-custom-app"
        dist_dir = external_dir / "dist"
        dist_dir.mkdir(parents=True)
        (dist_dir / "index.html").write_text("<html></html>")
        (external_dir / "package.json").write_text("{}")
        (external_dir / "package-lock.json").write_text("{}")

        modules_dir = tmp_path / "modules" / "my_module" / "apps"
        modules_dir.mkdir(parents=True)
        yaml_file = modules_dir / "my-app.AppVersion.yaml"
        yaml_file.write_text("")
        item = {
            "appExternalId": "my-app",
            "version": "1.0.0",
            "sourcePath": "../../../my-custom-app",
        }

        extras = list(
            AppVersionIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item)
        )

        assert len(extras) == 1
        assert extras[0].suffix == ".zip"

    @pytest.mark.parametrize(
        "extra_files, expected_in_error",
        [
            pytest.param({}, "missing package.json", id="package-json-missing"),
            pytest.param({"package.json": "{}"}, "package-lock.json", id="package-lock-json-missing"),
            pytest.param(
                {"package.json": "{}", "package-lock.json": "{}", "manifest.json": "not valid json{"},
                "invalid manifest.json",
                id="manifest-json-invalid",
            ),
        ],
    )
    def test_fails_on_missing_or_invalid_app_root_files(
        self, tmp_path: Path, extra_files: dict[str, str], expected_in_error: str
    ):
        app_dir = tmp_path / "my-app"
        dist_dir = app_dir / "dist"
        dist_dir.mkdir(parents=True)
        (dist_dir / "index.html").write_text("<html></html>")
        for filename, content in extra_files.items():
            (app_dir / filename).write_text(content)

        yaml_file = tmp_path / "my-app.AppVersion.yaml"
        yaml_file.write_text("")
        item = {"appExternalId": "my-app", "version": "1.0.0"}

        extras = list(
            AppVersionIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item)
        )

        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)
        assert expected_in_error in extras[0].error
