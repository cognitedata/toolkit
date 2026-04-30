"""Serve command for running a local development server for Function Apps."""

import os
import re
import shutil
import sys
import tempfile
import threading
import webbrowser
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
        """Start a local development server for testing a function app handler."""
        try:
            import uvicorn
        except ImportError:
            print(
                "[bold red]Error:[/] Missing dependencies for serve command.\n"
                "Install with: [bold]pip install cognite-toolkit\\[serve][/]\n"
                "Or with uv: [bold]uv pip install cognite-toolkit\\[serve][/]"
            )
            raise SystemExit(1)

        try:
            from cognite_function_apps.cli import _load_handler_from_path  # noqa: F401
            from cognite_function_apps.devserver import create_asgi_app  # noqa: F401
        except ImportError:
            print(
                "[bold red]Error:[/] Missing [bold]cognite-function-apps[/] package.\n"
                "Install with: [bold]pip install cognite-function-apps\\[cli][/]"
            )
            raise SystemExit(1)

        # If no path given, discover and prompt
        if path is None:
            path = self._prompt_function_selection()

        handler_path = path.resolve()
        self._validate_handler_directory(handler_path)
        self._validate_handler_is_function_app(handler_path)

        # Authenticate via the toolkit's standard auth path
        from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

        env_vars = EnvironmentVariables.create_from_environment()
        cdf_project = env_vars.CDF_PROJECT
        cdf_cluster = env_vars.CDF_CLUSTER

        # Check build type — block prod
        validation_type = self._load_validation_type()
        self._check_build_type(cdf_project, cdf_cluster, validation_type)

        url = f"http://{host}:{port}"
        print(f"\n[bold green]Starting server at {url}[/]")
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

    # ── Function discovery ──

    @staticmethod
    def _discover_function_dirs(organization_dir: Path | None = None) -> list[Path]:
        """Discover function app directories by scanning for handler.py files
        that import cognite_function_apps."""
        from cognite_toolkit._cdf_tk.utils.modules import iterate_modules

        root = organization_dir or Path.cwd()
        function_dirs: list[Path] = []

        for _module_dir, files in iterate_modules(root):
            for f in files:
                if f.name != "handler.py":
                    continue
                # Check if parent is inside a functions/ folder
                if f.parent.parent.name != "functions":
                    continue
                # Quick check: is this a Function App handler?
                try:
                    source = f.read_text()
                except OSError:
                    continue
                if "cognite_function_apps" in source or "create_function_service" in source:
                    function_dirs.append(f.parent)

        function_dirs.sort(key=lambda p: p.name)
        return function_dirs

    @staticmethod
    def _prompt_function_selection() -> Path:
        """Discover function apps and prompt the user to pick one."""
        from cognite_toolkit._cdf_tk.cdf_toml import CDFToml

        toml = CDFToml.load(Path.cwd())
        org_dir = toml.cdf.default_organization_dir

        dirs = ServeFunctionCommand._discover_function_dirs(org_dir)

        if not dirs:
            print(
                "[bold red]Error:[/] No Function App handlers found.\n"
                "Looked for handler.py files importing cognite_function_apps\n"
                f"under [bold]{org_dir}[/]."
            )
            raise SystemExit(1)

        if len(dirs) == 1:
            print(f"[blue]Found one function app:[/] [bold]{dirs[0].name}[/] ({dirs[0]})")
            return dirs[0]

        if not sys.stdin.isatty():
            print(
                "[bold red]Error:[/] Multiple function apps found but stdin is not interactive.\n"
                "Specify the function path explicitly: [bold]cdf dev function serve <path>[/]"
            )
            raise SystemExit(1)

        print("[bold]Available function apps:[/]\n")
        for i, d in enumerate(dirs, 1):
            # Show relative path from cwd for readability
            try:
                rel = d.relative_to(Path.cwd())
            except ValueError:
                rel = d
            print(f"  [bold cyan]{i}[/]  {d.name}  [dim]({rel})[/]")

        print()
        try:
            answer = input(f"Select function [1-{len(dirs)}]: ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            raise SystemExit(1)

        try:
            idx = int(answer) - 1
            if not 0 <= idx < len(dirs):
                raise ValueError
        except ValueError:
            print(f"[bold red]Error:[/] Invalid selection: {answer!r}")
            raise SystemExit(1)

        return dirs[idx]

    # ── Config & validation ──

    @staticmethod
    def _load_validation_type() -> str:
        """Load the validation-type from the project's config YAML.

        Uses the same config file resolution as build/deploy:
        reads config.{env}.yaml from the organization directory.
        Falls back to CDF_BUILD_TYPE env var, then 'dev'.
        """
        try:
            from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
            from cognite_toolkit._cdf_tk.data_classes._config_yaml import BuildConfigYAML

            toml = CDFToml.load(Path.cwd())
            build_env = toml.cdf.default_env
            org_dir = toml.cdf.default_organization_dir
            config = BuildConfigYAML.load_from_directory(org_dir, build_env)
            return config.environment.validation_type
        except Exception:
            # Fall back to env var
            return os.environ.get("CDF_BUILD_TYPE", "").strip().lower() or "dev"

    @staticmethod
    def _check_build_type(cdf_project: str, cdf_cluster: str, validation_type: str) -> None:
        """Check validation type and prompt the user to acknowledge the risk."""
        validation_type = validation_type.strip().lower()

        if validation_type == "prod":
            print(
                "[bold red]Error:[/] The dev server cannot run against a production configuration.\n"
                f"  validation-type = [bold]prod[/]\n"
                f"  CDF_PROJECT     = [bold]{cdf_project}[/]\n"
                f"  CDF_CLUSTER     = [bold]{cdf_cluster}[/]\n\n"
                "The dev server gives handlers [bold]full read/write access[/] to CDF.\n"
                "Running against production risks accidental data mutation.\n\n"
                "Use a [bold]dev[/] or [bold]staging[/] configuration instead."
            )
            raise SystemExit(1)

        print(
            f"[bold yellow]⚠  The dev server will authenticate to CDF with full read/write access.[/]\n"
            f"   validation-type = [bold]{validation_type}[/]\n"
            f"   CDF_PROJECT     = [bold]{cdf_project}[/]\n"
            f"   CDF_CLUSTER     = [bold]{cdf_cluster}[/]\n"
        )

        # Non-interactive environments (CI, piped stdin) skip the prompt
        if not sys.stdin.isatty():
            return

        try:
            answer = input("Continue? [y/N] ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            raise SystemExit(1)

        if answer not in ("y", "yes"):
            raise SystemExit(0)

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
    def _validate_handler_is_function_app(handler_path: Path) -> None:
        """Check that handler.py defines a FunctionService handle (not a classical function)."""
        handler_file = handler_path / "handler.py"
        if not handler_file.is_file():
            print(f"[bold red]Error:[/] handler.py not found in {handler_path}")
            raise SystemExit(1)

        source = handler_file.read_text()

        # Quick heuristic: FunctionApp-based handlers import from cognite_function_apps
        # and call create_function_service(). Classical handlers define `def handle(client, data)`.
        has_function_apps_import = "cognite_function_apps" in source or "create_function_service" in source
        if not has_function_apps_import:
            print(
                "[bold red]Error:[/] This handler appears to be a classical Cognite Function, "
                "not a Function App.\n\n"
                "The dev server only supports Function Apps that use [bold]cognite-function-apps[/].\n"
                "Classical functions (with [bold]def handle(client, data)[/]) are not supported.\n\n"
                "See the Function Apps documentation for how to migrate."
            )
            raise SystemExit(1)

    @staticmethod
    def _detect_tracing(handle: object) -> tuple[bool, str]:
        """Detect if the loaded FunctionService uses tracing.

        Returns (tracing_enabled, backend_endpoint).
        """
        try:
            from cognite_function_apps.tracer import TracingApp

            # Walk the ASGI app chain looking for a TracingApp
            app = getattr(handle, "asgi_app", None)
            while app is not None:
                if isinstance(app, TracingApp):
                    # Try to get the endpoint from the exporter provider closure
                    endpoint = ""
                    try:
                        from cognite_function_apps.tracer import OTLP_BACKENDS

                        for _name, config in OTLP_BACKENDS.items():
                            if config.endpoint and hasattr(app, "_exporter_provider"):
                                closure = getattr(app._exporter_provider, "__closure__", None)
                                if closure:
                                    for cell in closure:
                                        cell_val = cell.cell_contents
                                        if hasattr(cell_val, "endpoint") and cell_val.endpoint == config.endpoint:
                                            endpoint = config.endpoint
                                            break
                            if endpoint:
                                break
                    except Exception:
                        pass
                    return True, endpoint
                app = getattr(app, "next_app", None)
        except ImportError:
            pass
        return False, ""

    @staticmethod
    def _patch_cognite_client_factory() -> None:
        """Monkey-patch cognite_function_apps to use the toolkit's auth path."""
        import importlib

        from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables

        asgi_module = importlib.import_module("cognite_function_apps.devserver.asgi")

        def _toolkit_get_client() -> object:
            env_vars = EnvironmentVariables.create_from_environment()
            return env_vars.get_client(is_strict_validation=False)

        asgi_module.get_cognite_client_from_env = _toolkit_get_client  # type: ignore[attr-defined]

    # ── Server startup ──

    @staticmethod
    def _run_server_with_reload(
        uvicorn: Any,
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

# Detect tracing from the loaded handler
_tracing_enabled, _tracing_endpoint = ServeFunctionCommand._detect_tracing(handler_module.handle)

app = LandingPageMiddleware(
    _inner_app,
    handler_name={handler_name!r},
    handler_path={str(handler_path / "handler.py")!r},
    cdf_project={cdf_project!r},
    cdf_cluster={cdf_cluster!r},
    tracing_enabled=_tracing_enabled,
    tracing_endpoint=_tracing_endpoint,
)
'''
        )

        temp_dir_added = temp_dir not in sys.path
        if temp_dir_added:
            sys.path.insert(0, temp_dir)

        try:
            uvicorn.run(
                "_cdf_serve_asgi_app:app",
                host=host,
                port=port,
                reload=True,
                reload_dirs=[str(handler_path)],
                log_level=log_level,
                timeout_graceful_shutdown=1,
            )
        finally:
            if temp_dir_added and temp_dir in sys.path:
                sys.path.remove(temp_dir)
            shutil.rmtree(temp_dir, ignore_errors=True)

    @staticmethod
    def _run_server_without_reload(
        uvicorn: Any,
        handler_path: Path,
        host: str,
        port: int,
        log_level: str,
        handler_name: str,
        cdf_project: str,
        cdf_cluster: str,
    ) -> None:
        """Run the development server without auto-reload."""
        from cognite_function_apps.cli import _load_handler_from_path
        from cognite_function_apps.devserver import create_asgi_app

        from ._landing_page import LandingPageMiddleware

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

            # Detect tracing
            tracing_enabled, tracing_endpoint = ServeFunctionCommand._detect_tracing(handle)

            print("[blue]Creating ASGI app...[/]")
            inner_app = create_asgi_app(handle)
            asgi_app = LandingPageMiddleware(
                inner_app,  # type: ignore[arg-type]
                handler_name=handler_name,
                handler_path=str(handler_path / "handler.py"),
                cdf_project=cdf_project,
                cdf_cluster=cdf_cluster,
                tracing_enabled=tracing_enabled,
                tracing_endpoint=tracing_endpoint,
            )
            print("[green]ASGI app created[/]")

            uvicorn.run(
                asgi_app,
                host=host,
                port=port,
                reload=False,
                log_level=log_level,
            )
        finally:
            if package_root_added and package_root in sys.path:
                sys.path.remove(package_root)
