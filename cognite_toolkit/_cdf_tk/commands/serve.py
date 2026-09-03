"""Serve Function Apps locally."""

import importlib.util
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from rich import print

from ._base import ToolkitCommand


class ServeFunctionCommand(ToolkitCommand):
    def serve(
        self,
        path: Path | None,
        host: str = "127.0.0.1",
        port: int = 8000,
        reload: bool = True,
        log_level: str = "info",
    ) -> None:
        """Start a local development server for a Function App."""
        try:
            import uvicorn
            from cognite_function_apps.devserver import create_asgi_app
        except ImportError:
            print(
                "[bold red]Error:[/] Missing dependencies for the serve command.\n"
                "Install with: [bold]uv sync --extra serve[/]"
            )
            raise SystemExit(1)

        if path is None:
            print("[bold red]Error:[/] Specify the directory containing handler.py.")
            raise SystemExit(1)

        handler_path = path.resolve()
        self._validate_handler_directory(handler_path)
        self._validate_function_app_handler(handler_path)

        from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

        environment = EnvironmentVariables.create_from_environment()
        validation_type = self._load_validation_type()
        self._validate_environment(environment.CDF_PROJECT, environment.CDF_CLUSTER, validation_type)

        if reload:
            self._run_with_reload(uvicorn, handler_path, host, port, log_level)
        else:
            self._run_without_reload(uvicorn, create_asgi_app, handler_path, host, port, log_level)

    @staticmethod
    def _run_without_reload(
        uvicorn: Any, create_asgi_app: Any, handler_path: Path, host: str, port: int, log_level: str
    ) -> None:
        original_path = sys.path.copy()
        try:
            ServeFunctionCommand._patch_cognite_client_factory()
            handle = ServeFunctionCommand._load_handler(handler_path)
            app = create_asgi_app(handle)
            uvicorn.run(app, host=host, port=port, log_level=log_level)
        finally:
            sys.path[:] = original_path

    @staticmethod
    def _load_handler(handler_path: Path) -> Any:
        handler_file = handler_path / "handler.py"
        module_name = f"{handler_path.name}.handler"
        spec = importlib.util.spec_from_file_location(module_name, handler_file)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Failed to load {handler_file}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        sys.path.insert(0, str(handler_path))
        spec.loader.exec_module(module)
        try:
            return module.handle
        except AttributeError as error:
            raise RuntimeError(f"{handler_file} does not define a handle") from error

    @staticmethod
    def _run_with_reload(uvicorn: Any, handler_path: Path, host: str, port: int, log_level: str) -> None:
        temp_dir = Path(tempfile.mkdtemp(prefix="cdf_serve_"))
        module_path = temp_dir / "_cdf_serve_asgi.py"
        package_root = handler_path.parent
        temp_dir_str = str(temp_dir)
        inserted_path = False
        try:
            module_path.write_text(
                "import importlib\n"
                "import sys\n"
                f"sys.path.insert(0, {str(package_root)!r})\n"
                f"sys.path.insert(0, {str(handler_path)!r})\n"
                "from cognite_function_apps.devserver import create_asgi_app\n"
                "from cognite_toolkit._cdf_tk.commands.serve import ServeFunctionCommand\n"
                "ServeFunctionCommand._patch_cognite_client_factory()\n"
                f"handle = importlib.import_module({handler_path.name!r} + '.handler').handle\n"
                "app = create_asgi_app(handle)\n"
            )
            sys.path.insert(0, temp_dir_str)
            inserted_path = True
            uvicorn.run(
                "_cdf_serve_asgi:app",
                host=host,
                port=port,
                reload=True,
                reload_dirs=[str(handler_path)],
                log_level=log_level,
            )
        finally:
            if inserted_path:
                sys.path.remove(temp_dir_str)
            shutil.rmtree(temp_dir)

    @staticmethod
    def _validate_handler_directory(handler_path: Path) -> None:
        if not handler_path.is_dir():
            print(f"[bold red]Error:[/] Path is not a directory: {handler_path}")
            raise SystemExit(1)
        if not handler_path.name.isidentifier():
            suggested_name = re.sub(r"\W|^(?=\d)", "_", handler_path.name)
            print(
                f"[bold red]Error:[/] Directory name '{handler_path.name}' is not a valid Python module name.\n"
                f"[yellow]Suggested name:[/] [green]{suggested_name}[/]"
            )
            raise SystemExit(1)
        if handler_path.name in sys.stdlib_module_names:
            print(f"[bold red]Error:[/] Directory name '{handler_path.name}' shadows a standard library module.")
            raise SystemExit(1)

    @staticmethod
    def _validate_function_app_handler(handler_path: Path) -> None:
        handler_file = handler_path / "handler.py"
        if not handler_file.is_file():
            print(f"[bold red]Error:[/] handler.py not found in {handler_path}")
            raise SystemExit(1)
        try:
            source = handler_file.read_text()
        except OSError as error:
            print(f"[bold red]Error:[/] Could not read {handler_file}: {error}")
            raise SystemExit(1) from error
        if "cognite_function_apps" not in source and "create_function_service" not in source:
            print(
                "[bold red]Error:[/] This handler is not a Function App. "
                "Classical functions with [bold]def handle(client, data)[/] are not supported."
            )
            raise SystemExit(1)

    @staticmethod
    def _load_validation_type() -> str:
        return os.environ.get("CDF_BUILD_TYPE", "").strip().lower() or "dev"

    @staticmethod
    def _validate_environment(cdf_project: str, cdf_cluster: str, validation_type: str) -> None:
        if validation_type == "prod":
            print(
                "[bold red]Error:[/] The dev server cannot run against production.\n"
                f"CDF_PROJECT = [bold]{cdf_project}[/]\nCDF_CLUSTER = [bold]{cdf_cluster}[/]"
            )
            raise SystemExit(1)

    @staticmethod
    def _patch_cognite_client_factory() -> None:
        import importlib

        from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

        asgi_module = importlib.import_module("cognite_function_apps.devserver.asgi")

        def get_client() -> object:
            return EnvironmentVariables.create_from_environment().get_client(is_strict_validation=False)

        asgi_module.get_cognite_client_from_env = get_client  # type: ignore[attr-defined]
