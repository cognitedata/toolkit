import sys
from pathlib import Path

from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.commands.serve import ServeFunctionCommand


@pytest.fixture
def function_app_path(tmp_path: Path) -> Path:
    path = tmp_path / "my_function"
    path.mkdir()
    (path / "handler.py").write_text("from cognite_function_apps import create_function_service\n")
    return path


class TestServeFunctionCommand:
    def test_runs_with_reload(self, function_app_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        environment = MagicMock(CDF_PROJECT="test-project", CDF_CLUSTER="westeurope-1")
        uvicorn = MagicMock()
        monkeypatch.setenv("CDF_BUILD_TYPE", "dev")

        with (
            patch("uvicorn.run", uvicorn.run),
            patch(
                "cognite_toolkit._cdf_tk.utils.auth.EnvironmentVariables.create_from_environment",
                return_value=environment,
            ),
        ):
            command.serve(function_app_path, host="0.0.0.0", port=8080, log_level="debug")

        uvicorn.run.assert_called_once_with(
            "_cdf_serve_asgi:app",
            host="0.0.0.0",
            port=8080,
            reload=True,
            reload_dirs=[str(function_app_path)],
            log_level="debug",
        )
        assert all("cdf_serve_" not in entry for entry in sys.path)

    def test_runs_without_reload(self, function_app_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        environment = MagicMock(CDF_PROJECT="test-project", CDF_CLUSTER="westeurope-1")
        uvicorn = MagicMock()
        loader = MagicMock(return_value="handle")
        create_asgi_app = MagicMock(return_value="asgi-app")
        monkeypatch.setenv("CDF_BUILD_TYPE", "dev")

        with (
            patch("uvicorn.run", uvicorn.run),
            patch.object(ServeFunctionCommand, "_load_handler", loader),
            patch("cognite_function_apps.devserver.create_asgi_app", create_asgi_app),
            patch.object(ServeFunctionCommand, "_patch_cognite_client_factory"),
            patch(
                "cognite_toolkit._cdf_tk.utils.auth.EnvironmentVariables.create_from_environment",
                return_value=environment,
            ),
        ):
            command.serve(function_app_path, host="0.0.0.0", port=8080, reload=False, log_level="debug")

        loader.assert_called_once_with(function_app_path)
        create_asgi_app.assert_called_once_with("handle")
        uvicorn.run.assert_called_once_with("asgi-app", host="0.0.0.0", port=8080, log_level="debug")

    def test_restores_sys_path_when_handler_loading_fails(
        self, function_app_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        environment = MagicMock(CDF_PROJECT="test-project", CDF_CLUSTER="westeurope-1")
        original_path = sys.path.copy()
        monkeypatch.setenv("CDF_BUILD_TYPE", "dev")

        with (
            patch.object(ServeFunctionCommand, "_load_handler", side_effect=RuntimeError("bad handler")),
            patch.object(ServeFunctionCommand, "_patch_cognite_client_factory"),
            patch(
                "cognite_toolkit._cdf_tk.utils.auth.EnvironmentVariables.create_from_environment",
                return_value=environment,
            ),
            pytest.raises(RuntimeError, match="bad handler"),
        ):
            command.serve(function_app_path, reload=False)

        assert sys.path == original_path

    def test_removes_reload_module_when_server_start_fails(
        self, function_app_path: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        environment = MagicMock(CDF_PROJECT="test-project", CDF_CLUSTER="westeurope-1")
        uvicorn = MagicMock()
        temporary_module_directory = tmp_path / "cdf_serve_test"
        temporary_module_directory.mkdir()
        monkeypatch.setenv("CDF_BUILD_TYPE", "dev")

        with (
            patch("uvicorn.run", uvicorn.run),
            patch("tempfile.mkdtemp", return_value=str(temporary_module_directory)),
            patch(
                "cognite_toolkit._cdf_tk.utils.auth.EnvironmentVariables.create_from_environment",
                return_value=environment,
            ),
            pytest.raises(RuntimeError, match="server failed"),
        ):
            uvicorn.run.side_effect = RuntimeError("server failed")
            command.serve(function_app_path)

        assert not temporary_module_directory.exists()
        assert all("cdf_serve_test" not in entry for entry in sys.path)

    def test_removes_reload_directory_when_module_creation_fails(self, function_app_path: Path, tmp_path: Path) -> None:
        temporary_module_directory = tmp_path / "cdf_serve_test"
        temporary_module_directory.mkdir()
        uvicorn = MagicMock()

        with (
            patch("tempfile.mkdtemp", return_value=str(temporary_module_directory)),
            patch.object(Path, "write_text", side_effect=OSError("write failed")),
            pytest.raises(OSError, match="write failed"),
        ):
            ServeFunctionCommand._run_with_reload(uvicorn, function_app_path, "127.0.0.1", 8000, "info")

        assert not temporary_module_directory.exists()
        assert all("cdf_serve_test" not in entry for entry in sys.path)
        uvicorn.run.assert_not_called()

    def test_blocks_production_before_startup(self, function_app_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        environment = MagicMock(CDF_PROJECT="prod-project", CDF_CLUSTER="westeurope-1")
        monkeypatch.setenv("CDF_BUILD_TYPE", "prod")

        with (
            patch(
                "cognite_toolkit._cdf_tk.utils.auth.EnvironmentVariables.create_from_environment",
                return_value=environment,
            ),
            patch.object(ServeFunctionCommand, "_patch_cognite_client_factory") as patch_factory,
            pytest.raises(SystemExit),
        ):
            command.serve(function_app_path, reload=False)

        patch_factory.assert_not_called()
