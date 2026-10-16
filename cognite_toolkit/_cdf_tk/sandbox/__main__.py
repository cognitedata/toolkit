import argparse
from pathlib import Path

from dotenv import load_dotenv

from cognite_toolkit._cdf_tk.sandbox._flags import enable_all_flags

enable_all_flags()

from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables  # noqa: E402
from cognite_toolkit._cdf_tk.sandbox._server import make_server  # noqa: E402


def _client_from_args(env_file: Path | None):
    if env_file is not None:
        if not env_file.is_file():
            raise SystemExit(f"Env file not found: {env_file}")
        load_dotenv(env_file, override=False)
    try:
        return EnvironmentVariables.create_from_environment().get_client(is_strict_validation=False)
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the modeling sandbox HTTP server.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Optional .env file with CDF credentials. Enables /cdf/* routes.",
    )
    args = parser.parse_args()

    client = _client_from_args(args.env_file)
    server = make_server(args.host, args.port, client=client)
    suffix = " (CDF enabled)" if client is not None else " (YAML only; pass --env-file for CDF)"
    print(f"Sandbox server listening on http://{args.host}:{args.port}{suffix}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
