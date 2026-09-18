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

    uvicorn.reset_mock(side_effect=True)
    create_asgi_app = MagicMock(return_value="asgi-app")
    with (
        patch("uvicorn.run", uvicorn.run),
        patch.object(RunFunctionAppCommand, "_load_handler", return_value="handle"),
        patch("cognite_function_apps.devserver.create_asgi_app", create_asgi_app),
    ):
        command.run_function_app(function_app_path, reload=False)

    create_asgi_app.assert_called_once_with("handle", client_factory=RunFunctionAppCommand._create_cognite_client)


def test_loads_relative_imports_without_reload(tmp_path: Path) -> None:
    path = tmp_path / "function-app"
    path.mkdir()
    (path / "helper.py").write_text("handle = object()\n")
    (path / "handler.py").write_text("from .helper import handle\n")

    RunFunctionAppCommand._validate_handler_directory(path)
    RunFunctionAppCommand._run_without_reload(MagicMock(), MagicMock(), path, "127.0.0.1", 8000, "info")

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
