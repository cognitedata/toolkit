import io
import zipfile
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest
from cognite_toolkit._cdf_tk.client.resource_classes.app_version import AppVersionRequest, AppVersionResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.app import AppIO


def _make_app_request(
    external_id: str = "my-app",
    version: str = "1.0.0",
    name: str = "My App",
    lifecycle_state: str = "PUBLISHED",
    alias: str | None = None,
    entrypoint: str = "index.html",
) -> AppVersionRequest:
    return AppVersionRequest(
        external_id=external_id,
        version=version,
        name=name,
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


class TestAppIODeploy:
    @pytest.fixture
    def app_io_with_zip(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            zip_path = tmp_path / "1-my-app-my-app.zip"
            _write_zip(zip_path)
            version_id = AppVersionId(app_external_id="my-app", version="1.0.0")
            loader.zip_path_by_version_id[version_id] = zip_path
            yield loader, client

    def test_create_calls_create_and_upload(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="DRAFT", alias=None)

        loader.create([item])

        client.tool.apps.create.assert_called_once_with([AppRequest(external_id="my-app", name="My App")])
        client.tool.apps.versions.upload.assert_called_once_with(
            external_id="my-app",
            version="1.0.0",
            entrypoint="index.html",
            zip_bytes=loader.zip_path_by_version_id[
                AppVersionId(app_external_id="my-app", version="1.0.0")
            ].read_bytes(),
        )

    def test_deploy_sets_lifecycle_and_alias(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias="ACTIVE")

        loader.create([item])

        client.tool.apps.versions.update.assert_called_once_with(
            "my-app", "1.0.0", {"lifecycleState": {"set": "PUBLISHED"}, "alias": {"set": "ACTIVE"}}
        )

    def test_deploy_clears_alias_when_local_alias_is_none(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias=None)

        loader.create([item])

        client.tool.apps.versions.update.assert_called_once_with(
            "my-app", "1.0.0", {"lifecycleState": {"set": "PUBLISHED"}, "alias": {"setNull": True}}
        )

    def test_deploy_sets_preview_alias(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias="PREVIEW")

        loader.create([item])

        client.tool.apps.versions.update.assert_called_once_with(
            "my-app", "1.0.0", {"lifecycleState": {"set": "PUBLISHED"}, "alias": {"set": "PREVIEW"}}
        )

    def test_deploy_raises_when_zip_missing(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            item = _make_app_request(external_id="missing-app")
            with pytest.raises(ToolkitRequiredValueError, match="missing-app"):
                loader.create([item])

    def test_deploy_returns_response_with_correct_fields(self, app_io_with_zip):
        loader, _client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias="ACTIVE")

        results = loader.create([item])

        assert len(results) == 1
        response = results[0]
        assert isinstance(response, AppVersionResponse)
        assert response.app_external_id == "my-app"
        assert response.version == "1.0.0"
        assert response.lifecycle_state == "PUBLISHED"
        assert response.alias == "ACTIVE"

    def test_update_calls_create_and_upload(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(version="2.0.0", lifecycle_state="DRAFT", alias=None)
        # Register zip for 2.0.0
        zip_path = loader.zip_path_by_version_id[AppVersionId(app_external_id="my-app", version="1.0.0")]
        loader.zip_path_by_version_id[AppVersionId(app_external_id="my-app", version="2.0.0")] = zip_path
        loader.update([item])

        client.tool.apps.create.assert_called_once_with([AppRequest(external_id="my-app", name="My App")])
        client.tool.apps.versions.upload.assert_called_once()

    def test_delete_calls_delete_version_grouped_by_app(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            ids = [
                AppVersionId(app_external_id="my-app", version="1.0.0"),
                AppVersionId(app_external_id="my-app", version="2.0.0"),
            ]
            loader.delete(ids)

        client.tool.apps.versions.delete.assert_called_once_with(ids)


class TestAppIOGetId:
    @pytest.mark.parametrize("ext_key", ["externalId", "appExternalId", "external_id", "app_external_id"])
    def test_from_dict_all_key_variants(self, ext_key: str):
        assert AppIO.get_id({ext_key: "my-app", "version": "1.0.0"}) == AppVersionId(
            app_external_id="my-app", version="1.0.0"
        )

    @pytest.mark.parametrize(
        "item, match",
        [
            ({"version": "1.0.0"}, "externalId"),
            ({"externalId": "my-app"}, "version"),
        ],
    )
    def test_from_dict_raises_when_field_missing(self, item: dict, match: str):
        with pytest.raises(ToolkitRequiredValueError, match=match):
            AppIO.get_id(item)

    @pytest.mark.parametrize(
        "item",
        [
            AppVersionRequest(external_id="my-app", version="1.0.0", name="My App"),
            AppVersionResponse(app_external_id="my-app", version="1.0.0", lifecycle_state="DRAFT"),
        ],
    )
    def test_from_resource_object(self, item: AppVersionRequest | AppVersionResponse):
        assert AppIO.get_id(item) == AppVersionId(app_external_id="my-app", version="1.0.0")


class TestAppIOLoadResourceFile:
    def test_registers_zip_path_for_valid_yaml(self, tmp_path: Path):
        apps_dir = tmp_path / "apps"
        apps_dir.mkdir()
        yaml_file = apps_dir / "my-app.App.yaml"
        yaml_file.write_text("externalId: my-app\nversion: 1.0.0\nname: My App\n")

        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            result = loader.load_resource_file(yaml_file)

        assert result == [{"externalId": "my-app", "version": "1.0.0", "name": "My App"}]
        version_id = AppVersionId(app_external_id="my-app", version="1.0.0")
        assert version_id in loader.zip_path_by_version_id
        assert loader.zip_path_by_version_id[version_id] == apps_dir / "my-app.zip"

    def test_returns_empty_when_parent_not_apps(self, tmp_path: Path):
        other_dir = tmp_path / "other"
        other_dir.mkdir()
        yaml_file = other_dir / "my-app.App.yaml"
        yaml_file.write_text("externalId: my-app\nversion: 1.0.0\nname: My App\n")

        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            result = loader.load_resource_file(yaml_file)

        assert result == []


class TestAppIORetrieveAndIterate:
    def test_retrieve_returns_matching_responses(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            version_response = _make_app_response(app_external_id="my-app", version="1.0.0")
            client.tool.apps.versions.retrieve.return_value = [version_response]
            ids = [AppVersionId(app_external_id="my-app", version="1.0.0")]

            result = loader.retrieve(ids)

        assert len(result) == 1
        assert result[0].app_external_id == "my-app"

    def test_retrieve_skips_not_found(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            client.tool.apps.versions.retrieve.return_value = []
            ids = [AppVersionId(app_external_id="missing", version="1.0.0")]

            result = loader.retrieve(ids)

        assert result == []

    def test_iterate_yields_all_pages(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            page = [_make_app_response()]
            client.tool.apps.versions.iterate.return_value = iter([page])

            result = list(loader._iterate())

        assert result == page

    def test_delete_empty_list_returns_zero(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            result = loader.delete([])

        assert result == 0
        client.tool.apps.versions.delete.assert_not_called()


class TestAppIODumpResource:
    def test_uses_local_name_and_description_when_immutable_drift(self):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, None)

        response = AppVersionResponse(
            app_external_id="my-app",
            version="1.0.0",
            lifecycle_state="PUBLISHED",
            alias="ACTIVE",
        )
        local = {"name": "New local name", "description": "New description"}

        dumped = loader.dump_resource(response, local=local)

        assert dumped["externalId"] == "my-app"
        assert dumped["name"] == "New local name"
        assert dumped["description"] == "New description"

    def test_copies_source_path_from_local(self):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, None)

        response = AppVersionResponse(
            app_external_id="my-app",
            version="1.0.0",
            lifecycle_state="PUBLISHED",
            alias="ACTIVE",
        )
        local = {"sourcePath": "../../../../my-custom-app"}

        dumped = loader.dump_resource(response, local=local)

        assert dumped["sourcePath"] == "../../../../my-custom-app"


class TestAppIOGetExtraFiles:
    def test_yields_zip_with_dist_contents(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        dist_dir = app_dir / "dist"
        dist_dir.mkdir(parents=True)
        (dist_dir / "index.html").write_text("<html></html>")
        (dist_dir / "bundle.js").write_text("console.log('hi')")
        (app_dir / "package.json").write_text("{}")
        (app_dir / "package-lock.json").write_text("{}")

        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "my-app", "version": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        extra = extras[0]
        assert extra.suffix == ".zip"
        assert extra.byte_content is not None
        with zipfile.ZipFile(io.BytesIO(extra.byte_content)) as zf:
            names = zf.namelist()
        assert any("index.html" in n for n in names)
        assert any("bundle.js" in n for n in names)
        assert "package.json" in names
        assert "package-lock.json" in names

    def test_falls_back_to_root_without_dist(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        app_dir.mkdir()
        (app_dir / "index.html").write_text("<html></html>")
        (app_dir / "package.json").write_text("{}")
        (app_dir / "package-lock.json").write_text("{}")

        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "my-app", "version": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        assert extras[0].suffix == ".zip"

    def test_fails_when_entrypoint_missing_from_root_and_dist(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        app_dir.mkdir()
        # No index.html at root, no dist/, no src/+package.json

        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "my-app", "version": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)
        assert "index.html" in extras[0].error

    def test_fails_with_build_hint_when_unbuilt_webapp(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        app_dir.mkdir()
        (app_dir / "src").mkdir()
        (app_dir / "package.json").write_text("{}")
        (app_dir / "package-lock.json").write_text("{}")
        (app_dir / "index.html").write_text("<html></html>")  # Vite template at root

        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "my-app", "version": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)
        assert "npm run build" in extras[0].error

    def test_fails_when_app_dir_missing(self, tmp_path: Path):
        yaml_file = tmp_path / "missing-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "missing-app", "version": "1.0.0", "name": "Missing App"}

        extras = list(
            AppIO.get_extra_files(yaml_file, AppVersionId(app_external_id="missing-app", version="1.0.0"), item)
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
        yaml_file = modules_dir / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {
            "externalId": "my-app",
            "version": "1.0.0",
            "name": "My App",
            "sourcePath": "../../../my-custom-app",
        }

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        assert extras[0].suffix == ".zip"

    def test_fails_when_app_dir_missing_from_source_path(self, tmp_path: Path):
        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "my-app", "version": "1.0.0", "name": "My App", "sourcePath": "does-not-exist"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(app_external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)
