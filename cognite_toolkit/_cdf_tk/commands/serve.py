"""Serve command for running a local development server for Function Apps."""

import re
import shutil
import sys
import tempfile
import threading
import webbrowser
from pathlib import Path

from rich import print

from ._base import ToolkitCommand


class ServeFunctionCommand(ToolkitCommand):
    def serve(
        self,
        path: Path,
        host: str = "127.0.0.1",
        port: int = 8000,
        reload: bool = True,
        log_level: str = "info",
    ) -> None:
        """Start a local development server for testing a function app handler."""
        try:
            import uvicorn  # noqa: PLC0415
        except ImportError:
            print(
                "[bold red]Error:[/] Missing dependencies for serve command.\n"
                "Install with: [bold]pip install cognite-toolkit\\[serve][/]\n"
                "Or with uv: [bold]uv pip install cognite-toolkit\\[serve][/]"
            )
            raise SystemExit(1)

        try:
            from cognite_function_apps.cli import _load_handler_from_path  # noqa: F401, PLC0415
            from cognite_function_apps.devserver import create_asgi_app  # noqa: F401, PLC0415
        except ImportError:
            print(
                "[bold red]Error:[/] Missing [bold]cognite-function-apps[/] package.\n"
                "Install with: [bold]pip install cognite-function-apps\\[cli][/]"
            )
            raise SystemExit(1)

        handler_path = path.resolve()
        self._validate_handler_directory(handler_path)

        # Authenticate via the toolkit's standard auth path
        from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables  # noqa: PLC0415

        env_vars = EnvironmentVariables.create_from_environment()
        cdf_project = env_vars.CDF_PROJECT
        cdf_cluster = env_vars.CDF_CLUSTER

        url = f"http://{host}:{port}"
        print()
        print(
            f"[bold yellow]⚠  Dev server will authenticate to project "
            f"'{cdf_project}' on cluster '{cdf_cluster}'.[/]"
        )
        print("[bold yellow]   Handlers can read AND WRITE data.[/]")
        print()
        print(f"[bold green]Starting server at {url}[/]")
        if reload:
            print("[yellow]Auto-reload enabled — watching for changes...[/]")
        print("[yellow]Press CTRL+C to quit[/]\n")

        # Open browser after a short delay so uvicorn has time to bind
        threading.Timer(1.5, webbrowser.open, args=(url,)).start()

        handler_name = handler_path.name

        if reload:
            self._run_server_with_reload(
                uvicorn, handler_path, host, port, log_level, handler_name, cdf_project, cdf_cluster
            )
        else:
            self._run_server_without_reload(
                uvicorn, handler_path, host, port, log_level, handler_name, cdf_project, cdf_cluster
            )

    @staticmethod
    def _validate_handler_directory(handler_path: Path) -> None:
        """Validate that the handler directory exists and is valid."""
        if not handler_path.exists():
            print(f"[bold red]Error:[/] Path does not exist: {handler_path}")
            raise SystemExit(1)

        if not handler_path.is_dir():
            print(f"[bold red]Error:[/] Path is not a directory: {handler_path}")
            raise SystemExit(1)

        dir_name = handler_path.name
        if not dir_name.isidentifier():
            suggested_name = re.sub(r"\W|^(?=\d)", "_", dir_name)
            print(
                f"[bold red]Error:[/] Directory name '{dir_name}' is not a valid Python module name.\n"
                f"[yellow]Suggested name:[/] [green]{suggested_name}[/]"
            )
            raise SystemExit(1)

        if dir_name in sys.stdlib_module_names:
            print(
                f"[bold red]Error:[/] Directory name '{dir_name}' shadows a standard library module.\n"
                "[yellow]Please rename the directory to avoid import conflicts.[/]"
            )
            raise SystemExit(1)

    @staticmethod
    def _patch_cognite_client_factory() -> None:
        """Monkey-patch cognite_function_apps to use the toolkit's auth path.

        The cognite_function_apps library creates its own CogniteClient via
        get_cognite_client_from_env() using COGNITE_* env vars. We replace that
        with the toolkit's EnvironmentVariables auth path so users only need to
        configure CDF_CLUSTER / CDF_PROJECT and their IDP credentials.
        """
        import importlib

        from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables  # noqa: PLC0415

        # Import the module directly — works both when it's already in sys.modules
        # and when called before create_asgi_app (e.g. in the reload subprocess).
        asgi_module = importlib.import_module("cognite_function_apps.devserver.asgi")

        def _toolkit_get_client() -> object:
            env_vars = EnvironmentVariables.create_from_environment()
            return env_vars.get_client(is_strict_validation=False)

        asgi_module.get_cognite_client_from_env = _toolkit_get_client  # type: ignore[assignment]

    @staticmethod
    def _run_server_with_reload(
        uvicorn: object,
        handler_path: Path,
        host: str,
        port: int,
        log_level: str,
        handler_name: str,
        cdf_project: str,
        cdf_cluster: str,
    ) -> None:
        """Run the development server with auto-reload enabled."""
        package_root = handler_path.parent
        package_name = re.sub(r"\W|^(?=\d)", "_", handler_path.name)

        temp_dir = tempfile.mkdtemp(prefix="cdf_serve_")
        temp_app_file = Path(temp_dir) / "_cdf_serve_asgi_app.py"
        temp_app_file.write_text(
            f'''"""Temporary ASGI app for cdf dev function serve with reload support."""
import sys
from pathlib import Path
import importlib

package_root = Path({str(package_root)!r})
if str(package_root) not in sys.path:
    sys.path.insert(0, str(package_root))

# Patch cognite_function_apps to use the toolkit's auth path
from cognite_toolkit._cdf_tk.commands.serve import ServeFunctionCommand
ServeFunctionCommand._patch_cognite_client_factory()

from cognite_function_apps.devserver import create_asgi_app
from cognite_toolkit._cdf_tk.commands._landing_page import LandingPageMiddleware

handler_module = importlib.import_module("{package_name}.handler")
_inner_app = create_asgi_app(handler_module.handle)
app = LandingPageMiddleware(
    _inner_app,
    handler_name={handler_name!r},
    handler_path={str(handler_path / "handler.py")!r},
    cdf_project={cdf_project!r},
    cdf_cluster={cdf_cluster!r},
)
'''
        )

        temp_dir_added = temp_dir not in sys.path
        if temp_dir_added:
            sys.path.insert(0, temp_dir)

        try:
            uvicorn.run(  # type: ignore[union-attr]
                "_cdf_serve_asgi_app:app",
                host=host,
                port=port,
                reload=True,
                reload_dirs=[str(handler_path)],
                log_level=log_level,
            )
        finally:
            if temp_dir_added and temp_dir in sys.path:
                sys.path.remove(temp_dir)
            shutil.rmtree(temp_dir, ignore_errors=True)

    @staticmethod
    def _run_server_without_reload(
        uvicorn: object,
        handler_path: Path,
        host: str,
        port: int,
        log_level: str,
        handler_name: str,
        cdf_project: str,
        cdf_cluster: str,
    ) -> None:
        """Run the development server without auto-reload."""
        from cognite_function_apps.cli import _load_handler_from_path  # noqa: PLC0415
        from cognite_function_apps.devserver import create_asgi_app  # noqa: PLC0415

        from ._landing_page import LandingPageMiddleware  # noqa: PLC0415

        package_root = str(handler_path.parent)
        package_root_added = package_root not in sys.path
        if package_root_added:
            sys.path.insert(0, package_root)

        try:
            # Patch before create_asgi_app calls get_cognite_client_from_env
            ServeFunctionCommand._patch_cognite_client_factory()

            print(f"[blue]Loading handler from {handler_path}/handler.py...[/]")
            handle = _load_handler_from_path(handler_path)
            print("[green]Handler loaded successfully[/]")

            print("[blue]Creating ASGI app...[/]")
            inner_app = create_asgi_app(handle)
            asgi_app = LandingPageMiddleware(
                inner_app,
                handler_name=handler_name,
                handler_path=str(handler_path / "handler.py"),
                cdf_project=cdf_project,
                cdf_cluster=cdf_cluster,
            )
            print("[green]ASGI app created[/]")

            uvicorn.run(  # type: ignore[union-attr]
                asgi_app,
                host=host,
                port=port,
                reload=False,
                log_level=log_level,
            )
        finally:
            if package_root_added and package_root in sys.path:
                sys.path.remove(package_root)
