import os
import subprocess
from contextlib import suppress
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Literal, TypeAlias, get_args

from cognite.client.config import global_config
from cognite.client.credentials import CredentialProvider, OAuthClientCredentials, OAuthInteractive, Token
from rich.prompt import Prompt

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.utils.auth import CLIENT_NAME

_LOGIN_FLOW: TypeAlias = Literal["infer", "client_credentials", "interactive", "token"]
_VALID_LOGIN_FLOWS = get_args(_LOGIN_FLOW)


def get_toolkit_client(env_file_name: str, enable_set_pending_ids: bool = False) -> ToolkitClient:
    """Instantiate a ToolkitClient using environment variables. If the environment variables are not set, the user will
    be prompted to enter them.

    Args:
        env_file_name: The name of the .env file to look for in the repository root / current working directory. If
        the file is found, the variables will be loaded from the file. If the file is not found, the user will
        be prompted to enter the variables and the file will be created.
        enable_set_pending_ids: Whether to enable the set_pending_ids method on the client.

    Returns:
        ToolkitClient: A ToolkitClient instance.

    """
    if not env_file_name.endswith(".env"):
        raise ValueError("env_file_name must end with '.env'")
    global_config.disable_pypi_version_check = True
    # First try to load from .env file in repository root
    repo_root = _repo_root()
    if repo_root:
        with suppress(KeyError, FileNotFoundError, TypeError):
            variables = _from_dotenv(repo_root / env_file_name)
            client = variables.get_client(enable_set_pending_ids)
            print(f"Found {env_file_name} file in repository root. Loaded variables from {env_file_name} file.")
            return client
    elif (Path.cwd() / env_file_name).exists():
        with suppress(KeyError, FileNotFoundError, TypeError):
            variables = _from_dotenv(Path.cwd() / env_file_name)
            client = variables.get_client(enable_set_pending_ids)
            print(
                f"Found {env_file_name} file in current working directory. Loaded variables from {env_file_name} file."
            )
            return client
    # Then try to load from environment variables
    with suppress(KeyError):
        variables = EnvironmentVariables.create_from_environ()
        print("Loaded variables from environment variables.")
        return variables.get_client(enable_set_pending_ids)
    # If not found, prompt the user
    variables = _prompt_user()
    if repo_root and _env_in_gitignore(repo_root, env_file_name):
        env_file = repo_root / env_file_name
        location = "repository root"
    elif repo_root:
        # We do not offer to create the file in the repository root if it is in .gitignore
        # as an inexperienced user might accidentally commit it.
        print("Cannot create .env file in repository root as there is no .env entry in the .gitignore.")
        return variables.get_client(enable_set_pending_ids)
    else:
        env_file = Path.cwd() / env_file_name
        location = "current working directory"

    answer = Prompt.ask(
        f"Do you store the variables in an {env_file_name} file in the {location} for easy reuse?",
        choices=["y", "n"],
    )
    if env_file.exists():
        answer = Prompt.ask(f"{env_file} already exists. Overwrite?", choices=["y", "n"])
    if answer == "y":
        env_file.write_text(variables.create_env_file())
        print(f"Created {env_file_name} file in {location}.")

    return variables.get_client(enable_set_pending_ids)


@dataclass
class EnvironmentVariables:
    CDF_CLUSTER: str
    CDF_PROJECT: str
    LOGIN_FLOW: _LOGIN_FLOW = "infer"
    IDP_CLIENT_ID: str | None = None
    IDP_CLIENT_SECRET: str | None = None
    CDF_TOKEN: str | None = None

    IDP_TENANT_ID: str | None = None
    IDP_TOKEN_URL: str | None = None

    CDF_URL: str | None = None
    IDP_AUDIENCE: str | None = None
    IDP_SCOPES: str | None = None
    IDP_AUTHORITY_URL: str | None = None
    CDF_MAX_WORKERS: int | None = None
    CDF_TIMEOUT: int | None = None
    CDF_REDIRECT_PORT: int = 53_000

    def __post_init__(self):
        if self.LOGIN_FLOW.lower() not in _VALID_LOGIN_FLOWS:
            raise ValueError(f"LOGIN_FLOW must be one of {_VALID_LOGIN_FLOWS}")
        if self.IDP_TOKEN_URL and not self.IDP_TENANT_ID:
            prefix, suffix = "https://login.microsoftonline.com/", "/oauth2/v2.0/token"
            if self.IDP_TOKEN_URL.startswith(prefix) and self.IDP_TOKEN_URL.endswith(suffix):
                self.IDP_TENANT_ID = self.IDP_TOKEN_URL.removeprefix(prefix).removesuffix(suffix)

    @property
    def cdf_url(self) -> str:
        return self.CDF_URL or f"https://{self.CDF_CLUSTER}.cognitedata.com"

    @property
    def idp_token_url(self) -> str:
        if self.IDP_TOKEN_URL:
            return self.IDP_TOKEN_URL
        if not self.IDP_TENANT_ID:
            raise KeyError("IDP_TENANT_ID or IDP_TOKEN_URL must be set in the environment.")
        return f"https://login.microsoftonline.com/{self.IDP_TENANT_ID}/oauth2/v2.0/token"

    @property
    def idp_audience(self) -> str:
        return self.IDP_AUDIENCE or f"https://{self.CDF_CLUSTER}.cognitedata.com"

    @property
    def idp_scopes(self) -> list[str]:
        if self.IDP_SCOPES:
            return self.IDP_SCOPES.split(",")
        return [f"https://{self.CDF_CLUSTER}.cognitedata.com/.default"]

    @property
    def idp_authority_url(self) -> str:
        if self.IDP_AUTHORITY_URL:
            return self.IDP_AUTHORITY_URL
        if not self.IDP_TENANT_ID:
            raise KeyError("IDP_TENANT_ID or IDP_AUTHORITY_URL must be set in the environment.")
        return f"https://login.microsoftonline.com/{self.IDP_TENANT_ID}"

    @classmethod
    def create_from_environ(cls) -> "EnvironmentVariables":
        if "CDF_CLUSTER" not in os.environ or "CDF_PROJECT" not in os.environ:
            raise KeyError("CDF_CLUSTER and CDF_PROJECT must be set in the environment.", "CDF_CLUSTER", "CDF_PROJECT")

        return cls(
            CDF_CLUSTER=os.environ["CDF_CLUSTER"],
            CDF_PROJECT=os.environ["CDF_PROJECT"],
            LOGIN_FLOW=os.environ.get("LOGIN_FLOW", "infer"),  # type: ignore[arg-type]
            IDP_CLIENT_ID=os.environ.get("IDP_CLIENT_ID"),
            IDP_CLIENT_SECRET=os.environ.get("IDP_CLIENT_SECRET"),
            CDF_TOKEN=os.environ.get("CDF_TOKEN"),
            CDF_URL=os.environ.get("CDF_URL"),
            IDP_TOKEN_URL=os.environ.get("IDP_TOKEN_URL"),
            IDP_TENANT_ID=os.environ.get("IDP_TENANT_ID"),
            IDP_AUDIENCE=os.environ.get("IDP_AUDIENCE"),
            IDP_SCOPES=os.environ.get("IDP_SCOPES"),
            IDP_AUTHORITY_URL=os.environ.get("IDP_AUTHORITY_URL"),
            CDF_MAX_WORKERS=int(os.environ["CDF_MAX_WORKERS"]) if "CDF_MAX_WORKERS" in os.environ else None,
            CDF_TIMEOUT=int(os.environ["CDF_TIMEOUT"]) if "CDF_TIMEOUT" in os.environ else None,
            CDF_REDIRECT_PORT=int(os.environ.get("CDF_REDIRECT_PORT", 53_000)),
        )

    @classmethod
    def default(cls) -> "EnvironmentVariables":
        # This method is for backwards compatibility with the old config
        # It is not recommended to use this method.
        return cls(
            LOGIN_FLOW="client_credentials",
            CDF_CLUSTER="api",
            CDF_PROJECT="dev",
            IDP_TENANT_ID="common",
            IDP_CLIENT_ID="neat",
            IDP_CLIENT_SECRET="secret",
            IDP_SCOPES="project:read,project:write",
            CDF_TIMEOUT=60,
            CDF_MAX_WORKERS=3,
        )

    def get_credentials(self) -> CredentialProvider:
        method_by_flow = {
            "client_credentials": self.get_oauth_client_credentials,
            "interactive": self.get_oauth_interactive,
            "token": self.get_token,
        }
        if self.LOGIN_FLOW in method_by_flow:
            return method_by_flow[self.LOGIN_FLOW]()
        key_options: list[tuple[str, ...]] = []
        for method in method_by_flow.values():
            try:
                return method()
            except KeyError as e:
                key_options += e.args[1:]
        raise KeyError(
            f"LOGIN_FLOW={self.LOGIN_FLOW} requires one of the following environment set variables to be set.",
            *key_options,
        )

    def get_oauth_client_credentials(self) -> OAuthClientCredentials:
        if not self.IDP_CLIENT_ID or not self.IDP_CLIENT_SECRET:
            raise KeyError(
                "IDP_CLIENT_ID and IDP_CLIENT_SECRET must be set in the environment.",
                "IDP_CLIENT_ID",
                "IDP_CLIENT_SECRET",
            )
        return OAuthClientCredentials(
            client_id=self.IDP_CLIENT_ID,
            client_secret=self.IDP_CLIENT_SECRET,
            token_url=self.idp_token_url,
            audience=self.idp_audience,
            scopes=self.idp_scopes,
        )

    def get_oauth_interactive(self) -> OAuthInteractive:
        if not self.IDP_CLIENT_ID:
            raise KeyError("IDP_CLIENT_ID must be set in the environment.", "IDP_CLIENT_ID")
        return OAuthInteractive(
            client_id=self.IDP_CLIENT_ID,
            authority_url=self.idp_authority_url,
            redirect_port=self.CDF_REDIRECT_PORT,
            scopes=self.idp_scopes,
        )

    def get_token(self) -> Token:
        if not self.CDF_TOKEN:
            raise KeyError("TOKEN must be set in the environment", "TOKEN")
        return Token(self.CDF_TOKEN)

    def get_client(self, enable_set_pending_ids: bool = False) -> ToolkitClient:
        config = ToolkitClientConfig(
            client_name=CLIENT_NAME,
            project=self.CDF_PROJECT,
            credentials=self.get_credentials(),
            base_url=self.cdf_url,
            max_workers=self.CDF_MAX_WORKERS,
            timeout=self.CDF_TIMEOUT,
        )
        return ToolkitClient(config, enable_set_pending_ids)

    def create_env_file(self) -> str:
        lines: list[str] = []
        first_optional = True
        for field in fields(self):
            is_optional = hasattr(self, field.name.lower())
            if is_optional and first_optional:
                lines.append(
                    "# The below variables are the defaults, they are automatically constructed unless they are set."
                )
                first_optional = False
            name = field.name.lower() if is_optional else field.name
            value = getattr(self, name)
            if value is not None:
                if isinstance(value, list):
                    value = ",".join(value)
                lines.append(f"{field.name}={value}")
        return "\n".join(lines)


def _from_dotenv(evn_file: Path) -> EnvironmentVariables:
    if not evn_file.exists():
        raise FileNotFoundError(f"{evn_file} does not exist.")
    content = evn_file.read_text()
    valid_variables = {f.name for f in fields(EnvironmentVariables)}
    variables: dict[str, str] = {}
    for line in content.splitlines():
        if line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in valid_variables:
            variables[key] = value
    return EnvironmentVariables(**variables)  # type: ignore[arg-type]


def _prompt_user() -> EnvironmentVariables:
    try:
        variables = EnvironmentVariables.create_from_environ()
        continue_ = Prompt.ask(
            f"Use environment variables for CDF Cluster '{variables.CDF_CLUSTER}' "
            f"and Project '{variables.CDF_PROJECT}'? [y/n]",
            choices=["y", "n"],
            default="y",
        )
        if continue_ == "n":
            variables = _prompt_cluster_and_project()
    except KeyError:
        variables = _prompt_cluster_and_project()

    login_flow = Prompt.ask("Login flow", choices=[f for f in _VALID_LOGIN_FLOWS if f != "infer"])
    variables.LOGIN_FLOW = login_flow  # type: ignore[assignment]
    if login_flow == "token":
        token = Prompt.ask("Enter token")
        variables.CDF_TOKEN = token
        return variables

    variables.IDP_CLIENT_ID = Prompt.ask("Enter IDP Client ID")
    if login_flow == "client_credentials":
        variables.IDP_CLIENT_SECRET = Prompt.ask("Enter IDP Client Secret", password=True)
        tenant_id = Prompt.ask("Enter IDP_TENANT_ID (leave empty to enter IDP_TOKEN_URL instead)")
        if tenant_id:
            variables.IDP_TENANT_ID = tenant_id
        else:
            token_url = Prompt.ask("Enter IDP_TOKEN_URL")
            variables.IDP_TOKEN_URL = token_url
        optional = ["IDP_AUDIENCE", "IDP_SCOPES"]
    else:  # login_flow == "interactive"
        tenant_id = Prompt.ask("Enter IDP_TENANT_ID (leave empty to enter IDP_AUTHORITY_URL instead)")
        if tenant_id:
            variables.IDP_TENANT_ID = tenant_id
        else:
            variables.IDP_AUTHORITY_URL = Prompt.ask("Enter IDP_TOKEN_URL")
        optional = ["IDP_SCOPES"]

    defaults = "".join(f"\n - {name}: {getattr(variables, name.lower())}" for name in optional)
    use_defaults = Prompt.ask(
        f"Use default values for the following variables?{defaults}", choices=["y", "n"], default="y"
    )
    if use_defaults:
        return variables
    for name in optional:
        value = Prompt.ask(f"Enter {name}")
        setattr(variables, name, value)
    return variables


def _prompt_cluster_and_project() -> EnvironmentVariables:
    from rich.prompt import Prompt

    cluster = Prompt.ask("Enter CDF Cluster (example 'greenfield', 'bluefield', 'westeurope-1)")
    project = Prompt.ask("Enter CDF Project")
    return EnvironmentVariables(cluster, project)


def _repo_root() -> Path | None:
    with suppress(Exception):
        result = subprocess.run("git rev-parse --show-toplevel".split(), stdout=subprocess.PIPE)
        if (output := result.stdout.decode().strip()) != "":
            return Path(output)
    return None


def _env_in_gitignore(repo_root: Path, env_file_name: str) -> bool:
    ignore_file = repo_root / ".gitignore"
    if not ignore_file.exists():
        return False
    else:
        ignored = {line.strip() for line in ignore_file.read_text().splitlines()}
        return env_file_name in ignored or "*.env" in ignored
