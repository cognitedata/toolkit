import asyncio
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.commands.run_function_app import RunFunctionAppCommand


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
        "cognite_toolkit._cdf_tk.commands.auth.EnvironmentVariables.create_from_environment",
        return_value=environment,
    ):
        yield environment


class TestRunFunctionAppCommand:
    def test_runs_with_reload(self, function_app_path: Path, tmp_path: Path) -> None:
        command = RunFunctionAppCommand(client=None, skip_tracking=True)
        uvicorn = MagicMock()
        temporary_module_directory = tmp_path / "cdf_run_function_app_reload"
        temporary_module_directory.mkdir()
        generated_module_content = {}

        def capture_generated_module(*_args: object, **_kwargs: object) -> None:
            generated_module_content["text"] = (
                temporary_module_directory / "_cdf_run_function_app_asgi.py"
            ).read_text()

        uvicorn.run.side_effect = capture_generated_module

        with (
            patch("uvicorn.run", uvicorn.run),
            patch("tempfile.mkdtemp", return_value=str(temporary_module_directory)),
        ):
            command.run_function_app(function_app_path, host="0.0.0.0", port=8080, log_level="debug")

        uvicorn.run.assert_called_once_with(
            "_cdf_run_function_app_asgi:app",
            host="0.0.0.0",
            port=8080,
            reload=True,
            reload_dirs=[str(function_app_path)],
            log_level="debug",
        )
        assert all("cdf_run_function_app_" not in entry for entry in sys.path)
        assert "test-project" in generated_module_content["text"]
        assert "westeurope-1" in generated_module_content["text"]
        assert "_wrap_with_landing_page" in generated_module_content["text"]

    def test_runs_without_reload(self, function_app_path: Path) -> None:
        command = RunFunctionAppCommand(client=None, skip_tracking=True)
        uvicorn = MagicMock()
        loader = MagicMock(return_value="handle")
        create_asgi_app = MagicMock(return_value="asgi-app")
        wrap_with_landing_page = MagicMock(return_value="wrapped-asgi-app")

        with (
            patch("uvicorn.run", uvicorn.run),
            patch.object(RunFunctionAppCommand, "_load_handler", loader),
            patch("cognite_function_apps.devserver.create_asgi_app", create_asgi_app),
            patch.object(RunFunctionAppCommand, "_patch_cognite_client_factory"),
            patch.object(RunFunctionAppCommand, "_wrap_with_landing_page", wrap_with_landing_page),
        ):
            command.run_function_app(function_app_path, host="0.0.0.0", port=8080, reload=False, log_level="debug")

        loader.assert_called_once_with(function_app_path)
        create_asgi_app.assert_called_once_with("handle")
        wrap_with_landing_page.assert_called_once_with("asgi-app", "test-project", "westeurope-1")
        uvicorn.run.assert_called_once_with("wrapped-asgi-app", host="0.0.0.0", port=8080, log_level="debug")

    def test_loads_relative_imports_without_reload(self, tmp_path: Path) -> None:
        function_app_path = tmp_path / "relative_import_app"
        function_app_path.mkdir()
        (function_app_path / "helper.py").write_text("handle = object()\n")
        (function_app_path / "handler.py").write_text("from .helper import handle\n")
        original_path = sys.path.copy()
        uvicorn = MagicMock()
        create_asgi_app = MagicMock(return_value="asgi-app")

        with (
            patch.object(RunFunctionAppCommand, "_patch_cognite_client_factory"),
            patch.object(RunFunctionAppCommand, "_wrap_with_landing_page", return_value="wrapped-asgi-app"),
        ):
            RunFunctionAppCommand._run_without_reload(
                uvicorn, create_asgi_app, function_app_path, "127.0.0.1", 8000, "info", "test-project", "westeurope-1"
            )

        assert sys.path == original_path
        create_asgi_app.assert_called_once()
        sys.modules.pop("relative_import_app.handler", None)
        sys.modules.pop("relative_import_app.helper", None)
        sys.modules.pop("relative_import_app", None)

    def test_restores_sys_path_when_handler_loading_fails(self, function_app_path: Path) -> None:
        command = RunFunctionAppCommand(client=None, skip_tracking=True)
        original_path = sys.path.copy()

        with (
            patch.object(RunFunctionAppCommand, "_load_handler", side_effect=RuntimeError("bad handler")),
            patch.object(RunFunctionAppCommand, "_patch_cognite_client_factory"),
            pytest.raises(RuntimeError, match="bad handler"),
        ):
            command.run_function_app(function_app_path, reload=False)

        assert sys.path == original_path

    def test_removes_reload_module_when_server_start_fails(self, function_app_path: Path, tmp_path: Path) -> None:
        command = RunFunctionAppCommand(client=None, skip_tracking=True)
        uvicorn = MagicMock()
        temporary_module_directory = tmp_path / "cdf_run_function_app_test"
        temporary_module_directory.mkdir()

        with (
            patch("uvicorn.run", uvicorn.run),
            patch("tempfile.mkdtemp", return_value=str(temporary_module_directory)),
            pytest.raises(RuntimeError, match="server failed"),
        ):
            uvicorn.run.side_effect = RuntimeError("server failed")
            command.run_function_app(function_app_path)

        assert not temporary_module_directory.exists()
        assert all("cdf_run_function_app_test" not in entry for entry in sys.path)

    def test_removes_reload_directory_when_module_creation_fails(self, function_app_path: Path, tmp_path: Path) -> None:
        temporary_module_directory = tmp_path / "cdf_run_function_app_test"
        temporary_module_directory.mkdir()
        uvicorn = MagicMock()

        with (
            patch("tempfile.mkdtemp", return_value=str(temporary_module_directory)),
            patch.object(Path, "write_text", side_effect=OSError("write failed")),
            pytest.raises(OSError, match="write failed"),
        ):
            RunFunctionAppCommand._run_with_reload(
                uvicorn, function_app_path, "127.0.0.1", 8000, "info", "test-project", "westeurope-1"
            )

        assert not temporary_module_directory.exists()
        assert all("cdf_run_function_app_test" not in entry for entry in sys.path)
        uvicorn.run.assert_not_called()

    def test_warns_when_host_is_not_loopback(self, function_app_path: Path) -> None:
        command = RunFunctionAppCommand(client=None, skip_tracking=True)

        with (
            patch("uvicorn.run", MagicMock()),
            patch.object(RunFunctionAppCommand, "_load_handler", return_value="handle"),
            patch("cognite_function_apps.devserver.create_asgi_app", return_value="asgi-app"),
            patch.object(RunFunctionAppCommand, "_patch_cognite_client_factory"),
            patch("cognite_toolkit._cdf_tk.commands.run_function_app.print") as mock_print,
        ):
            command.run_function_app(function_app_path, host="0.0.0.0", reload=False)

        assert any(
            "0.0.0.0" in str(call.args[0]) and "network" in str(call.args[0]) for call in mock_print.call_args_list
        )

    def test_no_warning_for_loopback_host(self, function_app_path: Path) -> None:
        command = RunFunctionAppCommand(client=None, skip_tracking=True)

        with (
            patch("uvicorn.run", MagicMock()),
            patch.object(RunFunctionAppCommand, "_load_handler", return_value="handle"),
            patch("cognite_function_apps.devserver.create_asgi_app", return_value="asgi-app"),
            patch.object(RunFunctionAppCommand, "_patch_cognite_client_factory"),
            patch("cognite_toolkit._cdf_tk.commands.run_function_app.print") as mock_print,
        ):
            command.run_function_app(function_app_path, reload=False)

        assert not any("network" in str(call.args[0]) for call in mock_print.call_args_list)


class TestSafetyBanner:
    def test_render_includes_project_and_cluster_and_warning(self) -> None:
        text = RunFunctionAppCommand._render_safety_banner("my-project", "my-cluster").decode()

        assert "my-project" in text
        assert "my-cluster" in text
        assert "create, update, or delete data" in text

    def test_render_escapes_html_in_project_name(self) -> None:
        text = RunFunctionAppCommand._render_safety_banner("<script>evil</script>", "cluster").decode()

        assert "<script>" not in text
        assert "&lt;script&gt;" in text

    def test_inject_inserts_banner_right_after_body_tag(self) -> None:
        page = b"<html><body><div>docs</div></body></html>"

        injected = RunFunctionAppCommand._inject_safety_banner(page, "my-project", "my-cluster")

        assert injected.startswith(b"<html><body><div style=")
        assert b"my-project" in injected
        assert b"<div>docs</div></body></html>" in injected

    def test_inject_is_a_noop_when_body_tag_is_missing(self) -> None:
        page = b"not html"

        injected = RunFunctionAppCommand._inject_safety_banner(page, "my-project", "my-cluster")

        assert injected == page

    def test_wrapper_redirects_root_to_docs(self) -> None:
        inner_app = MagicMock()
        wrapped = RunFunctionAppCommand._wrap_with_landing_page(inner_app, "my-project", "my-cluster")
        sent: list[dict] = []

        async def send(message: dict) -> None:
            sent.append(message)

        asyncio.run(wrapped({"type": "http", "path": "/", "method": "GET"}, None, send))

        inner_app.assert_not_called()
        assert sent[0]["status"] == 302
        assert (b"location", b"/docs") in sent[0]["headers"]

    def test_wrapper_injects_banner_into_docs_and_fixes_content_length(self) -> None:
        original_body = b"<html><body>swagger ui</body></html>"

        async def inner_app(scope: dict, receive: Any, send: Any) -> None:
            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [(b"content-type", b"text/html"), (b"content-length", str(len(original_body)).encode())],
                }
            )
            await send({"type": "http.response.body", "body": original_body, "more_body": False})

        wrapped = RunFunctionAppCommand._wrap_with_landing_page(inner_app, "my-project", "my-cluster")
        sent: list[dict] = []

        async def send(message: dict) -> None:
            sent.append(message)

        asyncio.run(wrapped({"type": "http", "path": "/docs", "method": "GET"}, None, send))

        start, body = sent
        assert start["status"] == 200
        assert b"my-project" in body["body"]
        sent_content_length = int(dict(start["headers"])[b"content-length"])
        assert sent_content_length == len(body["body"])

    def test_wrapper_passes_through_other_paths(self) -> None:
        async def inner_app(scope: dict, receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"inner"})

        wrapped = RunFunctionAppCommand._wrap_with_landing_page(inner_app, "my-project", "my-cluster")
        sent: list[dict] = []

        async def send(message: dict) -> None:
            sent.append(message)

        asyncio.run(wrapped({"type": "http", "path": "/health", "method": "GET"}, None, send))

        assert sent[1]["body"] == b"inner"
