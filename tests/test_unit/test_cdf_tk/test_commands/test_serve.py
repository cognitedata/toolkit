import asyncio
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.commands.serve import ServeFunctionCommand


@pytest.fixture
def function_app_path(tmp_path: Path) -> Path:
    path = tmp_path / "my_function"
    path.mkdir()
    (path / "handler.py").write_text("from cognite_function_apps import create_function_service\n")
    return path


@pytest.fixture(autouse=True)
def mock_environment_variables() -> Iterator[MagicMock]:
    environment = MagicMock(CDF_PROJECT="test-project", CDF_CLUSTER="westeurope-1")
    with patch(
        "cognite_toolkit._cdf_tk.utils.auth.EnvironmentVariables.create_from_environment",
        return_value=environment,
    ):
        yield environment


class TestServeFunctionCommand:
    def test_runs_with_reload(self, function_app_path: Path, tmp_path: Path) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        uvicorn = MagicMock()
        temporary_module_directory = tmp_path / "cdf_serve_reload"
        temporary_module_directory.mkdir()
        generated_module_content = {}

        def capture_generated_module(*_args: object, **_kwargs: object) -> None:
            generated_module_content["text"] = (temporary_module_directory / "_cdf_serve_asgi.py").read_text()

        uvicorn.run.side_effect = capture_generated_module

        with (
            patch("uvicorn.run", uvicorn.run),
            patch("tempfile.mkdtemp", return_value=str(temporary_module_directory)),
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
        assert "test-project" in generated_module_content["text"]
        assert "westeurope-1" in generated_module_content["text"]
        assert "_wrap_with_landing_page" in generated_module_content["text"]

    def test_runs_without_reload(self, function_app_path: Path) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        uvicorn = MagicMock()
        loader = MagicMock(return_value="handle")
        create_asgi_app = MagicMock(return_value="asgi-app")
        wrap_with_landing_page = MagicMock(return_value="wrapped-asgi-app")

        with (
            patch("uvicorn.run", uvicorn.run),
            patch.object(ServeFunctionCommand, "_load_handler", loader),
            patch("cognite_function_apps.devserver.create_asgi_app", create_asgi_app),
            patch.object(ServeFunctionCommand, "_patch_cognite_client_factory"),
            patch.object(ServeFunctionCommand, "_wrap_with_landing_page", wrap_with_landing_page),
        ):
            command.serve(function_app_path, host="0.0.0.0", port=8080, reload=False, log_level="debug")

        loader.assert_called_once_with(function_app_path)
        create_asgi_app.assert_called_once_with("handle")
        wrap_with_landing_page.assert_called_once_with("asgi-app", "test-project", "westeurope-1")
        uvicorn.run.assert_called_once_with("wrapped-asgi-app", host="0.0.0.0", port=8080, log_level="debug")

    def test_restores_sys_path_when_handler_loading_fails(self, function_app_path: Path) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        original_path = sys.path.copy()

        with (
            patch.object(ServeFunctionCommand, "_load_handler", side_effect=RuntimeError("bad handler")),
            patch.object(ServeFunctionCommand, "_patch_cognite_client_factory"),
            pytest.raises(RuntimeError, match="bad handler"),
        ):
            command.serve(function_app_path, reload=False)

        assert sys.path == original_path

    def test_removes_reload_module_when_server_start_fails(self, function_app_path: Path, tmp_path: Path) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)
        uvicorn = MagicMock()
        temporary_module_directory = tmp_path / "cdf_serve_test"
        temporary_module_directory.mkdir()

        with (
            patch("uvicorn.run", uvicorn.run),
            patch("tempfile.mkdtemp", return_value=str(temporary_module_directory)),
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
            ServeFunctionCommand._run_with_reload(
                uvicorn, function_app_path, "127.0.0.1", 8000, "info", "test-project", "westeurope-1"
            )

        assert not temporary_module_directory.exists()
        assert all("cdf_serve_test" not in entry for entry in sys.path)
        uvicorn.run.assert_not_called()

    def test_warns_when_host_is_not_loopback(self, function_app_path: Path) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)

        with (
            patch("uvicorn.run", MagicMock()),
            patch.object(ServeFunctionCommand, "_load_handler", return_value="handle"),
            patch("cognite_function_apps.devserver.create_asgi_app", return_value="asgi-app"),
            patch.object(ServeFunctionCommand, "_patch_cognite_client_factory"),
            patch("cognite_toolkit._cdf_tk.commands.serve.print") as mock_print,
        ):
            command.serve(function_app_path, host="0.0.0.0", reload=False)

        assert any(
            "0.0.0.0" in str(call.args[0]) and "network" in str(call.args[0]) for call in mock_print.call_args_list
        )

    def test_no_warning_for_loopback_host(self, function_app_path: Path) -> None:
        command = ServeFunctionCommand(client=None, skip_tracking=True)

        with (
            patch("uvicorn.run", MagicMock()),
            patch.object(ServeFunctionCommand, "_load_handler", return_value="handle"),
            patch("cognite_function_apps.devserver.create_asgi_app", return_value="asgi-app"),
            patch.object(ServeFunctionCommand, "_patch_cognite_client_factory"),
            patch("cognite_toolkit._cdf_tk.commands.serve.print") as mock_print,
        ):
            command.serve(function_app_path, reload=False)

        assert not any("network" in str(call.args[0]) for call in mock_print.call_args_list)


class TestLandingPage:
    def test_render_includes_project_and_cluster_and_warning(self) -> None:
        text = ServeFunctionCommand._render_landing_page("my-project", "my-cluster").decode()

        assert "my-project" in text
        assert "my-cluster" in text
        assert "create, update, or delete data" in text

    def test_render_escapes_html_in_project_name(self) -> None:
        text = ServeFunctionCommand._render_landing_page("<script>evil</script>", "cluster").decode()

        assert "<script>" not in text
        assert "&lt;script&gt;" in text

    def test_wrapper_serves_landing_page_at_root(self) -> None:
        inner_app = MagicMock()
        wrapped = ServeFunctionCommand._wrap_with_landing_page(inner_app, "my-project", "my-cluster")
        sent: list[dict] = []

        async def send(message: dict) -> None:
            sent.append(message)

        asyncio.run(wrapped({"type": "http", "path": "/", "method": "GET"}, None, send))

        inner_app.assert_not_called()
        assert sent[0]["status"] == 200
        assert b"my-project" in sent[1]["body"]

    def test_wrapper_passes_through_other_paths(self) -> None:
        async def inner_app(scope: dict, receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"inner"})

        wrapped = ServeFunctionCommand._wrap_with_landing_page(inner_app, "my-project", "my-cluster")
        sent: list[dict] = []

        async def send(message: dict) -> None:
            sent.append(message)

        asyncio.run(wrapped({"type": "http", "path": "/docs", "method": "GET"}, None, send))

        assert sent[1]["body"] == b"inner"
