from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.app import AppRequest, AppResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError, ToolkitRequiredValueError
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


class TestAppIODeploy:
    @pytest.fixture
    def app_io_with_dir(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            app_dir = tmp_path / "my-app"
            app_dir.mkdir()
            (app_dir / "index.html").write_text("<html></html>")
            loader.app_dir_by_external_id["my-app"] = app_dir
            yield loader, client

    def test_create_calls_ensure_and_upload(self, app_io_with_dir):
        loader, client = app_io_with_dir
        item = _make_app_request(published=False)

        with patch("cognite_toolkit._cdf_tk.resource_ios._resource_ios.app.create_temporary_zip") as mock_zip:
            zip_path = MagicMock(spec=Path)
            zip_path.read_bytes.return_value = b"ZIPCONTENT"
            mock_zip.return_value.__enter__ = MagicMock(return_value=zip_path)
            mock_zip.return_value.__exit__ = MagicMock(return_value=False)
            loader.create([item])

        client.tool.apps.ensure_app.assert_called_once_with(item)
        client.tool.apps.upload_version.assert_called_once_with(
            app_external_id="my-app",
            version_tag="1.0.0",
            entry_path="index.html",
            zip_bytes=b"ZIPCONTENT",
        )
        client.tool.apps.publish.assert_not_called()

    def test_create_publishes_when_published_true(self, app_io_with_dir):
        loader, client = app_io_with_dir
        item = _make_app_request(published=True)

        with patch("cognite_toolkit._cdf_tk.resource_ios._resource_ios.app.create_temporary_zip") as mock_zip:
            zip_path = MagicMock(spec=Path)
            zip_path.read_bytes.return_value = b"ZIPCONTENT"
            mock_zip.return_value.__enter__ = MagicMock(return_value=zip_path)
            mock_zip.return_value.__exit__ = MagicMock(return_value=False)
            loader.create([item])

        client.tool.apps.publish.assert_called_once_with("my-app", "1.0.0")

    def test_update_calls_ensure_and_upload(self, app_io_with_dir):
        loader, client = app_io_with_dir
        item = _make_app_request(version_tag="2.0.0", published=False)

        with patch("cognite_toolkit._cdf_tk.resource_ios._resource_ios.app.create_temporary_zip") as mock_zip:
            zip_path = MagicMock(spec=Path)
            zip_path.read_bytes.return_value = b"ZIPCONTENT"
            mock_zip.return_value.__enter__ = MagicMock(return_value=zip_path)
            mock_zip.return_value.__exit__ = MagicMock(return_value=False)
            loader.update([item])

        client.tool.apps.ensure_app.assert_called_once_with(item)
        client.tool.apps.upload_version.assert_called_once()
        client.tool.apps.publish.assert_not_called()

    def test_deploy_raises_when_app_dir_missing(self, tmp_path: Path):
        with monkeypatch_toolkit_client() as client:
            loader = AppIO.create_loader(client, tmp_path)
            item = _make_app_request(app_external_id="missing-app")
            with pytest.raises(ToolkitRequiredValueError, match="missing-app"):
                loader.create([item])

    def test_deploy_returns_response_with_correct_fields(self, app_io_with_dir):
        loader, _client = app_io_with_dir
        item = _make_app_request(published=True)

        with patch("cognite_toolkit._cdf_tk.resource_ios._resource_ios.app.create_temporary_zip") as mock_zip:
            zip_path = MagicMock(spec=Path)
            zip_path.read_bytes.return_value = b"ZIPCONTENT"
            mock_zip.return_value.__enter__ = MagicMock(return_value=zip_path)
            mock_zip.return_value.__exit__ = MagicMock(return_value=False)
            results = loader.create([item])

        assert len(results) == 1
        response = results[0]
        assert isinstance(response, AppResponse)
        assert response.app_external_id == "my-app"
        assert response.version_tag == "1.0.0"
        assert response.lifecycle_state == "PUBLISHED"
        assert response.alias == "ACTIVE"

    def test_deploy_response_draft_when_not_published(self, app_io_with_dir):
        loader, _client = app_io_with_dir
        item = _make_app_request(published=False)

        with patch("cognite_toolkit._cdf_tk.resource_ios._resource_ios.app.create_temporary_zip") as mock_zip:
            zip_path = MagicMock(spec=Path)
            zip_path.read_bytes.return_value = b"ZIPCONTENT"
            mock_zip.return_value.__enter__ = MagicMock(return_value=zip_path)
            mock_zip.return_value.__exit__ = MagicMock(return_value=False)
            results = loader.create([item])

        response = results[0]
        assert response.lifecycle_state == "DRAFT"
        assert response.alias is None


class TestAppBuilder:
    def test_copy_app_directory_uses_dist_when_present(self, tmp_path: Path):
        from cognite_toolkit._cdf_tk.builders._app import AppBuilder

        apps_dir = tmp_path / "apps"
        apps_dir.mkdir()
        # App dir must be a sibling of the YAML file (builder uses path.with_name(app_external_id))
        app_dir = apps_dir / "my-app"
        dist_dir = app_dir / "dist"
        dist_dir.mkdir(parents=True)
        (dist_dir / "index.html").write_text("<html></html>")
        (dist_dir / "bundle.js").write_text("console.log('hi')")

        yaml_file = apps_dir / "my-app.App.yaml"
        yaml_file.write_text("appExternalId: my-app\nversionTag: '1.0.0'\nname: My App\n")

        source_file = MagicMock()
        source_file.loaded = {"appExternalId": "my-app", "versionTag": "1.0.0", "name": "My App"}
        source_file.source.path = yaml_file

        build_dir = tmp_path / "build"
        build_dir.mkdir()
        builder = AppBuilder(build_dir=build_dir, warn=MagicMock())
        builder.copy_app_directory_to_build(source_file)

        copied = build_dir / "apps" / "my-app"
        assert (copied / "index.html").exists()
        assert (copied / "bundle.js").exists()
        assert not (copied / "dist").exists()

    def test_copy_app_directory_falls_back_to_root_without_dist(self, tmp_path: Path):
        from cognite_toolkit._cdf_tk.builders._app import AppBuilder

        apps_dir = tmp_path / "apps"
        apps_dir.mkdir()
        app_dir = apps_dir / "my-app"
        app_dir.mkdir()
        (app_dir / "index.html").write_text("<html></html>")
        (app_dir / "style.css").write_text("body {}")

        yaml_file = apps_dir / "my-app.App.yaml"
        yaml_file.write_text("appExternalId: my-app\nversionTag: '1.0.0'\nname: My App\n")

        source_file = MagicMock()
        source_file.loaded = {"appExternalId": "my-app", "versionTag": "1.0.0", "name": "My App"}
        source_file.source.path = yaml_file

        build_dir = tmp_path / "build"
        build_dir.mkdir()
        builder = AppBuilder(build_dir=build_dir, warn=MagicMock())
        builder.copy_app_directory_to_build(source_file)

        copied = build_dir / "apps" / "my-app"
        assert (copied / "index.html").exists()
        assert (copied / "style.css").exists()

    def test_copy_app_directory_raises_when_app_dir_missing(self, tmp_path: Path):
        from cognite_toolkit._cdf_tk.builders._app import AppBuilder

        apps_dir = tmp_path / "apps"
        apps_dir.mkdir()
        yaml_file = apps_dir / "missing-app.App.yaml"
        yaml_file.write_text("appExternalId: missing-app\nversionTag: '1.0.0'\nname: Missing App\n")

        source_file = MagicMock()
        source_file.loaded = {"appExternalId": "missing-app", "versionTag": "1.0.0", "name": "Missing App"}
        source_file.source.path = yaml_file

        build_dir = tmp_path / "build"
        build_dir.mkdir()
        builder = AppBuilder(build_dir=build_dir, warn=MagicMock())
        with pytest.raises(ToolkitNotADirectoryError, match="missing-app"):
            builder.copy_app_directory_to_build(source_file)

    def test_copy_app_directory_excludes_node_modules_and_git(self, tmp_path: Path):
        from cognite_toolkit._cdf_tk.builders._app import AppBuilder

        apps_dir = tmp_path / "apps"
        apps_dir.mkdir()
        app_dir = apps_dir / "my-app"
        app_dir.mkdir()
        (app_dir / "index.html").write_text("<html></html>")
        (app_dir / "node_modules").mkdir()
        (app_dir / "node_modules" / "pkg.js").write_text("module")
        (app_dir / ".git").mkdir()
        (app_dir / ".git" / "config").write_text("[core]")

        yaml_file = apps_dir / "my-app.App.yaml"
        yaml_file.write_text("appExternalId: my-app\nversionTag: '1.0.0'\nname: My App\n")

        source_file = MagicMock()
        source_file.loaded = {"appExternalId": "my-app", "versionTag": "1.0.0", "name": "My App"}
        source_file.source.path = yaml_file

        build_dir = tmp_path / "build"
        build_dir.mkdir()
        builder = AppBuilder(build_dir=build_dir, warn=MagicMock())
        builder.copy_app_directory_to_build(source_file)

        copied = build_dir / "apps" / "my-app"
        assert (copied / "index.html").exists()
        assert not (copied / "node_modules").exists()
        assert not (copied / ".git").exists()
