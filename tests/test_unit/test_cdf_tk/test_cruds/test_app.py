import io
import zipfile
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import AppVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError, ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.app import AppIO


def _make_app_request(
    external_id: str = "my-app",
    version: str = "1.0.0",
    name: str = "My App",
    lifecycle_state: str = "PUBLISHED",
    alias: str | None = None,
    entrypoint: str = "index.html",
) -> AppRequest:
    return AppRequest(
        external_id=external_id,
        version=version,
        name=name,
        lifecycle_state=lifecycle_state,
        alias=alias,
        entrypoint=entrypoint,
    )


def _make_app_response(
    external_id: str = "my-app",
    version: str = "1.0.0",
    lifecycle_state: str = "PUBLISHED",
    alias: str | None = "ACTIVE",
) -> AppResponse:
    return AppResponse(
        external_id=external_id,
        version=version,
        name="My App",
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
            version_id = AppVersionId(external_id="my-app", version="1.0.0")
            loader.zip_path_by_version_id[version_id] = zip_path
            yield loader, client

    def test_create_calls_ensure_and_upload(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="DRAFT", alias=None)
        client.tool.apps.retrieve_version.return_value = None

        loader.create([item])

        client.tool.apps.ensure_app.assert_called_once_with(item)
        client.tool.apps.upload_version.assert_called_once_with(
            external_id="my-app",
            version="1.0.0",
            entrypoint="index.html",
            zip_bytes=loader.zip_path_by_version_id[AppVersionId(external_id="my-app", version="1.0.0")].read_bytes(),
        )

    def test_deploy_promotes_draft_to_published_with_active_alias(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias="ACTIVE")
        client.tool.apps.retrieve_version.return_value = None

        loader.create([item])

        client.tool.apps.transition_lifecycle.assert_called_once_with("my-app", "1.0.0", "PUBLISHED")
        client.tool.apps.set_alias.assert_called_once_with("my-app", "1.0.0", "ACTIVE")

    def test_deploy_clears_alias_when_local_alias_is_none(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias=None)
        client.tool.apps.retrieve_version.return_value = _make_app_response(lifecycle_state="PUBLISHED", alias="ACTIVE")

        loader.create([item])

        client.tool.apps.transition_lifecycle.assert_not_called()
        client.tool.apps.set_alias.assert_called_once_with("my-app", "1.0.0", None)

    def test_deploy_swaps_alias_to_preview(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias="PREVIEW")
        client.tool.apps.retrieve_version.return_value = _make_app_response(lifecycle_state="PUBLISHED", alias="ACTIVE")

        loader.create([item])

        client.tool.apps.transition_lifecycle.assert_not_called()
        client.tool.apps.set_alias.assert_called_once_with("my-app", "1.0.0", "PREVIEW")

    def test_deploy_noop_when_lifecycle_and_alias_match(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias="ACTIVE")
        client.tool.apps.retrieve_version.return_value = _make_app_response(lifecycle_state="PUBLISHED", alias="ACTIVE")

        loader.create([item])

        client.tool.apps.transition_lifecycle.assert_not_called()
        client.tool.apps.set_alias.assert_not_called()

    def test_deploy_rejects_backward_lifecycle_transition(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="DRAFT", alias=None)
        client.tool.apps.retrieve_version.return_value = _make_app_response(lifecycle_state="PUBLISHED", alias=None)

        with pytest.raises(ToolkitValueError, match="forward-only"):
            loader.create([item])

    def test_deploy_rejects_alias_on_non_published_version(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(lifecycle_state="DRAFT", alias="ACTIVE")
        client.tool.apps.retrieve_version.return_value = None

        with pytest.raises(ToolkitValueError, match="alias"):
            loader.create([item])

    def test_deploy_raises_when_zip_missing(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            item = _make_app_request(external_id="missing-app")
            with pytest.raises(ToolkitRequiredValueError, match="missing-app"):
                loader.create([item])

    def test_deploy_returns_response_with_correct_fields(self, app_io_with_zip):
        loader, _client = app_io_with_zip
        item = _make_app_request(lifecycle_state="PUBLISHED", alias="ACTIVE")
        _client.tool.apps.retrieve_version.return_value = None

        results = loader.create([item])

        assert len(results) == 1
        response = results[0]
        assert isinstance(response, AppResponse)
        assert response.external_id == "my-app"
        assert response.version == "1.0.0"
        assert response.lifecycle_state == "PUBLISHED"
        assert response.alias == "ACTIVE"

    def test_update_calls_ensure_and_upload(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(version="2.0.0", lifecycle_state="DRAFT", alias=None)
        # Register zip for 2.0.0
        zip_path = loader.zip_path_by_version_id[AppVersionId(external_id="my-app", version="1.0.0")]
        loader.zip_path_by_version_id[AppVersionId(external_id="my-app", version="2.0.0")] = zip_path
        client.tool.apps.retrieve_version.return_value = None

        loader.update([item])

        client.tool.apps.ensure_app.assert_called_once_with(item)
        client.tool.apps.upload_version.assert_called_once()

    def test_delete_calls_delete_version_grouped_by_app(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            ids = [
                AppVersionId(external_id="my-app", version="1.0.0"),
                AppVersionId(external_id="my-app", version="2.0.0"),
            ]
            loader.delete(ids)

        client.tool.apps.delete_version.assert_called_once_with("my-app", ids)


class TestAppIODumpResource:
    def test_uses_local_name_and_description_when_immutable_drift(self):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, None)

        response = AppResponse(
            external_id="my-app",
            version="1.0.0",
            name="Old name from CDF",
            description=None,
            lifecycle_state="PUBLISHED",
            alias="ACTIVE",
        )
        local = {"name": "New local name", "description": "New description"}

        dumped = loader.dump_resource(response, local=local)

        assert dumped["name"] == "New local name"
        assert dumped["description"] == "New description"

    def test_copies_source_path_from_local(self):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, None)

        response = AppResponse(
            external_id="my-app",
            version="1.0.0",
            name="My App",
            lifecycle_state="PUBLISHED",
            alias="ACTIVE",
        )
        local = {"sourcePath": "../../../../my-dune-app"}

        dumped = loader.dump_resource(response, local=local)

        assert dumped["sourcePath"] == "../../../../my-dune-app"


class TestAppIOGetExtraFiles:
    def test_yields_zip_with_dist_contents(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        dist_dir = app_dir / "dist"
        dist_dir.mkdir(parents=True)
        (dist_dir / "index.html").write_text("<html></html>")
        (dist_dir / "bundle.js").write_text("console.log('hi')")

        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "my-app", "version": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        extra = extras[0]
        assert extra.suffix == ".zip"
        assert extra.byte_content is not None
        with zipfile.ZipFile(io.BytesIO(extra.byte_content)) as zf:
            names = zf.namelist()
        assert any("index.html" in n for n in names)
        assert any("bundle.js" in n for n in names)

    def test_falls_back_to_root_without_dist(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        app_dir.mkdir()
        (app_dir / "index.html").write_text("<html></html>")

        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "my-app", "version": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        assert extras[0].suffix == ".zip"

    def test_fails_when_app_dir_missing(self, tmp_path: Path):
        from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra

        yaml_file = tmp_path / "missing-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "missing-app", "version": "1.0.0", "name": "Missing App"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(external_id="missing-app", version="1.0.0"), item))

        assert len(extras) == 1
        assert isinstance(extras[0], FailedReadExtra)

    def test_uses_source_path_field(self, tmp_path: Path):
        external_dir = tmp_path / "my-dune-app"
        dist_dir = external_dir / "dist"
        dist_dir.mkdir(parents=True)
        (dist_dir / "index.html").write_text("<html></html>")

        modules_dir = tmp_path / "modules" / "my_module" / "apps"
        modules_dir.mkdir(parents=True)
        yaml_file = modules_dir / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {
            "externalId": "my-app",
            "version": "1.0.0",
            "name": "My App",
            "sourcePath": "../../../my-dune-app",
        }

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        assert extras[0].suffix == ".zip"

    def test_excludes_node_modules_and_git(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        app_dir.mkdir()
        (app_dir / "index.html").write_text("<html></html>")
        (app_dir / "node_modules").mkdir()
        (app_dir / "node_modules" / "pkg.js").write_text("module")
        (app_dir / ".git").mkdir()
        (app_dir / ".git" / "config").write_text("[core]")

        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"externalId": "my-app", "version": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, AppVersionId(external_id="my-app", version="1.0.0"), item))

        assert len(extras) == 1
        with zipfile.ZipFile(io.BytesIO(extras[0].byte_content)) as zf:  # type: ignore[arg-type]
            names = zf.namelist()
        assert not any("node_modules" in n for n in names)
        assert not any(".git" in n for n in names)
        assert any("index.html" in n for n in names)
