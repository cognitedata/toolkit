import os
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


def test_runs_with_reload(function_app_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(RunFunctionAppCommand._HANDLER_PATH_ENV_VAR, raising=False)
    command = RunFunctionAppCommand(client=None, skip_tracking=True)
    uvicorn = MagicMock()

    def assert_handler_path_is_set(*_args: object, **_kwargs: object) -> None:
        assert os.environ[RunFunctionAppCommand._HANDLER_PATH_ENV_VAR] == str(function_app_path)

    uvicorn.run.side_effect = assert_handler_path_is_set
    with patch("uvicorn.run", uvicorn.run):
        command.run_function_app(function_app_path, host="0.0.0.0", port=8080, log_level="debug")

    assert RunFunctionAppCommand._HANDLER_PATH_ENV_VAR not in os.environ
    uvicorn.run.assert_called_once_with(
        "cognite_toolkit._cdf_tk.commands.run_function_app:RunFunctionAppCommand._create_reloading_asgi_app",
        host="0.0.0.0",
        port=8080,
        reload=True,
        reload_dirs=[str(function_app_path)],
        log_level="debug",
        factory=True,
    )


def test_reload_restores_existing_handler_path(function_app_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(RunFunctionAppCommand._HANDLER_PATH_ENV_VAR, "previous-path")
    uvicorn = MagicMock()
    uvicorn.run.side_effect = RuntimeError("server failed")

    with pytest.raises(RuntimeError, match="server failed"):
        RunFunctionAppCommand._run_with_reload(uvicorn, function_app_path, "127.0.0.1", 8000, "info")

    assert os.environ[RunFunctionAppCommand._HANDLER_PATH_ENV_VAR] == "previous-path"


def test_reloading_asgi_factory(function_app_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(RunFunctionAppCommand._HANDLER_PATH_ENV_VAR, str(function_app_path))
    create_asgi_app = MagicMock(return_value="asgi-app")

    with (
        patch.object(RunFunctionAppCommand, "_load_handler", return_value="handle") as load_handler,
        patch("cognite_function_apps.devserver.create_asgi_app", create_asgi_app),
    ):
        app = RunFunctionAppCommand._create_reloading_asgi_app()

    assert app == "asgi-app"
    load_handler.assert_called_once_with(function_app_path)
    create_asgi_app.assert_called_once_with("handle", client_factory=RunFunctionAppCommand._create_cognite_client)


def test_loads_relative_imports(tmp_path: Path) -> None:
    path = tmp_path / "function-app"
    path.mkdir()
    (path / "helper.py").write_text("handle = object()\n")
    (path / "handler.py").write_text("from .helper import handle\n")

    try:
        assert RunFunctionAppCommand._load_handler(path) is not None
    finally:
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
