import os
from pathlib import Path


def get_cli_home() -> Path:
    if raw := os.environ.get("COGNITE_CLI_HOME", "").strip():
        return Path(raw).expanduser()
    return Path.home() / ".cognite-cli"


def session_file_path() -> Path:
    return get_cli_home() / "session.json"
