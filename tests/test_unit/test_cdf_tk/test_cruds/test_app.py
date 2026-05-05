import io
import zipfile
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.app import AppIO


def _make_app_request(
    app_external_id: str = "my-app",
    version_tag: str = "1.0.0",
    name: str = "My App",
    published: bool = False,
    entry_path: str = "index.html",
) -> AppRequest:
    return AppRequest(
        app_external_id=app_external_id,
        version_tag=version_tag,
        name=name,
        published=published,
        entry_path=entry_path,
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
            loader.zip_path_by_external_id["my-app"] = zip_path
            yield loader, client

    def test_create_calls_ensure_and_upload(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(published=False)
        loader.create([item])

        client.tool.apps.ensure_app.assert_called_once_with(item)
        client.tool.apps.upload_version.assert_called_once_with(
            app_external_id="my-app",
            version_tag="1.0.0",
            entry_path="index.html",
            zip_bytes=(loader.zip_path_by_external_id["my-app"].read_bytes()),
        )
        client.tool.apps.publish.assert_not_called()

    def test_create_publishes_when_published_true(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(published=True)
        loader.create([item])

        client.tool.apps.publish.assert_called_once_with("my-app", "1.0.0")

    def test_update_calls_ensure_and_upload(self, app_io_with_zip):
        loader, client = app_io_with_zip
        item = _make_app_request(version_tag="2.0.0", published=False)
        loader.update([item])

        client.tool.apps.ensure_app.assert_called_once_with(item)
        client.tool.apps.upload_version.assert_called_once()
        client.tool.apps.publish.assert_not_called()

    def test_deploy_raises_when_zip_missing(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            item = _make_app_request(app_external_id="missing-app")
            with pytest.raises(ToolkitRequiredValueError, match="missing-app"):
                loader.create([item])

    def test_deploy_returns_response_with_correct_fields(self, app_io_with_zip):
        loader, _client = app_io_with_zip
        item = _make_app_request(published=True)
        results = loader.create([item])

        assert len(results) == 1
        response = results[0]
        assert isinstance(response, AppResponse)
        assert response.app_external_id == "my-app"
        assert response.version_tag == "1.0.0"
        assert response.lifecycle_state == "PUBLISHED"
        assert response.alias == "ACTIVE"

    def test_deploy_response_draft_when_not_published(self, app_io_with_zip):
        loader, _client = app_io_with_zip
        item = _make_app_request(published=False)
        results = loader.create([item])

        response = results[0]
        assert response.lifecycle_state == "DRAFT"
        assert response.alias is None


class TestAppIOGetExtraFiles:
    def test_yields_zip_with_dist_contents(self, tmp_path: Path):
        app_dir = tmp_path / "my-app"
        dist_dir = app_dir / "dist"
        dist_dir.mkdir(parents=True)
        (dist_dir / "index.html").write_text("<html></html>")
        (dist_dir / "bundle.js").write_text("console.log('hi')")

        yaml_file = tmp_path / "my-app.App.yaml"
        yaml_file.write_text("")
        item = {"appExternalId": "my-app", "versionTag": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, ExternalId(external_id="my-app"), item))

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
        item = {"appExternalId": "my-app", "versionTag": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, ExternalId(external_id="my-app"), item))

        assert len(extras) == 1
        assert extras[0].suffix == ".zip"

    def test_fails_when_app_dir_missing(self, tmp_path: Path):
        from cognite_toolkit._cdf_tk.resource_ios._base_ios import FailedReadExtra

        yaml_file = tmp_path / "missing-app.App.yaml"
        yaml_file.write_text("")
        item = {"appExternalId": "missing-app", "versionTag": "1.0.0", "name": "Missing App"}

        extras = list(AppIO.get_extra_files(yaml_file, ExternalId(external_id="missing-app"), item))

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
            "appExternalId": "my-app",
            "versionTag": "1.0.0",
            "name": "My App",
            "sourcePath": "../../../my-dune-app",
        }

        extras = list(AppIO.get_extra_files(yaml_file, ExternalId(external_id="my-app"), item))

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
        item = {"appExternalId": "my-app", "versionTag": "1.0.0", "name": "My App"}

        extras = list(AppIO.get_extra_files(yaml_file, ExternalId(external_id="my-app"), item))

        assert len(extras) == 1
        with zipfile.ZipFile(io.BytesIO(extras[0].byte_content)) as zf:  # type: ignore[arg-type]
            names = zf.namelist()
        assert not any("node_modules" in n for n in names)
        assert not any(".git" in n for n in names)
        assert any("index.html" in n for n in names)
