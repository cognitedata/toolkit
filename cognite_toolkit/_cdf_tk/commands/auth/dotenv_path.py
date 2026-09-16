from pathlib import Path

from dotenv import dotenv_values

from cognite_toolkit._cdf_tk.commands.auth.data_classes._constants import parse_login_flow
from cognite_toolkit._cdf_tk.commands.auth.data_classes._environment_variables import EnvironmentVariables
from cognite_toolkit._cdf_tk.commands.auth.data_classes._types import LoginFlow
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible


def find_dotenv_path(cwd: Path | None = None) -> Path | None:
    """Return the .env file the CLI would load (cwd first, then parent)."""
    base = cwd or Path.cwd()
    for candidate in (base / ".env", base.parent / ".env"):
        if candidate.is_file():
            return candidate
    return None


def describe_configured_login_flow() -> tuple[LoginFlow | None, str | None]:
    """Return the active LOGIN_FLOW and where it came from (dotenv path or env var)."""
    env_flow = EnvironmentVariables.login_flow_from_environment()
    if env_flow is None:
        return None, None

    dotenv_path = find_dotenv_path()
    if dotenv_path is not None:
        file_raw = (dotenv_values(dotenv_path).get("LOGIN_FLOW") or "").strip()
        if file_raw:
            try:
                file_flow = parse_login_flow(file_raw)
            except ValueError:
                file_flow = None
            else:
                if file_flow == env_flow:
                    return env_flow, relative_to_if_possible(dotenv_path).as_posix()

    return env_flow, "the LOGIN_FLOW environment variable"
