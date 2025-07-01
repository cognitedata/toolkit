import os

from pathlib import Path
from dotenv import load_dotenv
from typing import Any, Tuple, Literal
from cognite.client import CogniteClient, ClientConfig, global_config
from cognite.client.credentials import OAuthClientCredentials

from utils.DataStructures import EnvConfig
from services.ConfigService import Config, load_config_parameters
from services.ConfigService import Config
from services.RetrieveService import GeneralRetrieveService
from services.ApplyService import GeneralApplyService
from services.LoggerService import CogniteFunctionLogger
from services.ReportService import GeneralReportService
from services.PipelineService import GeneralPipelineService


def get_env_variables() -> EnvConfig:
    print("Loading environment variables from .env...")

    project_path = (Path(__file__).parent / ".env").resolve()
    print(f"project_path is set to: {project_path}")

    load_dotenv()

    required_envvars = (
        "CDF_PROJECT",
        "CDF_CLUSTER",
        "IDP_TENANT_ID",
        "IDP_CLIENT_ID",
        "IDP_CLIENT_SECRET",
    )

    missing = [envvar for envvar in required_envvars if envvar not in os.environ]
    if missing:
        raise ValueError(f"Missing one or more env.vars: {missing}")

    return EnvConfig(
        cdf_project=os.getenv("CDF_PROJECT"),  # type: ignore
        cdf_cluster=os.getenv("CDF_CLUSTER"),  # type: ignore
        tenant_id=os.getenv("IDP_TENANT_ID"),  # type: ignore
        client_id=os.getenv("IDP_CLIENT_ID"),  # type: ignore
        client_secret=os.getenv("IDP_CLIENT_SECRET"),  # type: ignore
    )


def create_client(env_config: EnvConfig, debug: bool = False):
    settings = {
        "max_retries": 5,
        "disable_ssl": True,
    }
    global_config.apply_settings(settings)

    SCOPES = [f"https://{env_config.cdf_cluster}.cognitedata.com/.default"]
    TOKEN_URL = (
        f"https://login.microsoftonline.com/{env_config.tenant_id}/oauth2/v2.0/token"
    )
    creds = OAuthClientCredentials(
        token_url=TOKEN_URL,
        client_id=env_config.client_id,
        client_secret=env_config.client_secret,
        scopes=SCOPES,
    )
    cnf = ClientConfig(
        client_name="DEV_Working",
        project=env_config.cdf_project,
        base_url=f"https://{env_config.cdf_cluster}.cognitedata.com",  # NOTE: base_url might need to be adjusted if on PSAAS or Private Link
        credentials=creds,
        debug=debug,
    )
    client = CogniteClient(cnf)
    return client


def create_config_service(
    function_data: dict[str, Any], client: CogniteClient | None = None
) -> Tuple[Config, CogniteClient]:
    if client is None:
        env_config = get_env_variables()
        client = create_client(env_config)
    config = load_config_parameters(client=client, function_data=function_data)
    return config, client


def create_logger_service(log_level):
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        return CogniteFunctionLogger()
    else:
        return CogniteFunctionLogger(log_level=log_level)


def create_write_logger_service(log_level, filepath):
    if log_level not in ["DEBUG", "INFO", "WARNING", "ERROR"]:
        return CogniteFunctionLogger(write=True, filepath=filepath)
    else:
        return CogniteFunctionLogger(log_level=log_level, write=True, filepath=filepath)


def create_retrieve_service(
    client: CogniteClient, config: Config, logger: CogniteFunctionLogger
) -> GeneralRetrieveService:
    return GeneralRetrieveService(client, config, logger)


def create_report_service(
    client: CogniteClient, config: Config, logger: CogniteFunctionLogger
) -> GeneralReportService:
    return GeneralReportService(client, config, logger)


def create_apply_service(
    client: CogniteClient, config: Config, logger: CogniteFunctionLogger
) -> GeneralApplyService:
    return GeneralApplyService(client, config, logger)


def create_general_pipeline_service(
    client: CogniteClient, pipeline_ext_id: str
) -> GeneralPipelineService:
    return GeneralPipelineService(pipeline_ext_id, client)
