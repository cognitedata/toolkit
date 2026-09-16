import asyncio
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.commands.run_function_app import RunFunctionAppCommand


@pytest.fixture
def function_app_path(tmp_path: Path) -> Path:
    path = tmp_path / "my_function"
    path.mkdir()
    (path / "handler.py").write_text("from cognite_function_apps import create_function_service\n")
    return path


def test_runs_with_and_without_reload(function_app_path: Path, tmp_path: Path) -> None:
    command = RunFunctionAppCommand(client=None, skip_tracking=True)
    temporary_module_directory = tmp_path / "cdf_run_function_app"
    temporary_module_directory.mkdir()
    uvicorn = MagicMock()
    generated = {}
    uvicorn.run.side_effect = lambda *_args, **_kwargs: generated.setdefault(
        "module", (temporary_module_directory / "_cdf_run_function_app_asgi.py").read_text()
    )

    environment = MagicMock(CDF_PROJECT="project", CDF_CLUSTER="cluster")
    with (
        patch("uvicorn.run", uvicorn.run),
        patch("tempfile.mkdtemp", return_value=str(temporary_module_directory)),
        patch(
            "cognite_toolkit._cdf_tk.commands.auth.EnvironmentVariables.create_from_environment",
            return_value=environment,
        ),
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

    uvicorn.reset_mock(side_effect=True)
    with (
        patch("uvicorn.run", uvicorn.run),
        patch.object(RunFunctionAppCommand, "_load_handler", return_value="handle"),
        patch("cognite_function_apps.devserver.create_asgi_app", return_value="asgi-app"),
        patch.object(RunFunctionAppCommand, "_patch_cognite_client_factory"),
        patch.object(RunFunctionAppCommand, "_wrap_with_landing_page", return_value="wrapped"),
        patch(
            "cognite_toolkit._cdf_tk.commands.auth.EnvironmentVariables.create_from_environment",
            return_value=MagicMock(CDF_PROJECT="project", CDF_CLUSTER="cluster"),
        ),
    ):
        command.run_function_app(function_app_path, reload=False)


def test_loads_relative_imports_without_reload(tmp_path: Path) -> None:
    path = tmp_path / "function-app"
    path.mkdir()
    (path / "helper.py").write_text("handle = object()\n")
    (path / "handler.py").write_text("from .helper import handle\n")

    RunFunctionAppCommand._validate_handler_directory(path)
    with (
        patch.object(RunFunctionAppCommand, "_patch_cognite_client_factory"),
        patch.object(RunFunctionAppCommand, "_wrap_with_landing_page", return_value="wrapped"),
    ):
        RunFunctionAppCommand._run_without_reload(
            MagicMock(), MagicMock(), path, "127.0.0.1", 8000, "info", "project", "cluster"
        )

    for name in ["function-app.handler", "function-app.helper", "function-app"]:
        sys.modules.pop(name, None)


@pytest.mark.parametrize(
    ("path_name", "source", "message"),
    [
        ("json", "", "shadows a standard library module"),
        ("valid", "", "handler.py not found"),
        ("valid", "def handle(client, data): pass", "not a Function App"),
    ],
)
def test_rejects_invalid_handlers(tmp_path: Path, path_name: str, source: str, message: str) -> None:
    path = tmp_path / path_name
    path.mkdir()
    if source:
        (path / "handler.py").write_text(source)

    with pytest.raises(SystemExit), patch("cognite_toolkit._cdf_tk.commands.run_function_app.print") as output:
        RunFunctionAppCommand(client=None, skip_tracking=True).run_function_app(path)

    assert message in str(output.call_args)


def test_wraps_docs_and_redirects_root() -> None:
    async def app(_scope: dict, _receive: object, send: object) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": [(b"content-length", b"30")]})
        await send({"type": "http.response.body", "body": b"<html><body>docs</body></html>"})

    wrapped = RunFunctionAppCommand._wrap_with_landing_page(app, "<project>", "cluster")
    sent: list[dict] = []

    async def send(message: dict) -> None:
        sent.append(message)

    asyncio.run(wrapped({"type": "http", "path": "/", "method": "GET"}, None, send))
    assert sent[0]["status"] == 302
    sent.clear()
    asyncio.run(wrapped({"type": "http", "path": "/docs", "method": "GET"}, None, send))
    assert b"&lt;project&gt;" in sent[1]["body"]
    assert int(dict(sent[0]["headers"])[b"content-length"]) == len(sent[1]["body"])
