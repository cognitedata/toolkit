from dataclasses import Field
from typing import Any

import questionary
import typer
from questionary import Choice
from rich import print

from .data_classes import (
    LOGIN_FLOW_DESCRIPTION,
    PROVIDER_DESCRIPTION,
    VALID_LOGIN_FLOWS,
    EnvironmentVariables,
    LoginFlow,
    Provider,
)
from .data_classes._constants import parse_login_flow


def parse_login_flow_input(flow: str) -> LoginFlow:
    return parse_login_flow(flow)


def prompt_user_environment_variables(
    current: EnvironmentVariables | None = None,
    *,
    login_flow: LoginFlow | None = None,
) -> EnvironmentVariables:
    provider = questionary.select(
        "Choose the provider (Who authenticates you?)",
        choices=[
            Choice(title=f"{provider}: {description}", value=provider)
            for provider, description in PROVIDER_DESCRIPTION.items()
            if provider != "cdf"
        ],
        default=current.PROVIDER if current else "entra_id",
    ).unsafe_ask()
    exclude = set()
    if provider == "cdf":
        exclude = set(VALID_LOGIN_FLOWS) - {"client_credentials"}
    choices = [
        Choice(title=f"{flow}: {description}", value=flow)
        for flow, description in LOGIN_FLOW_DESCRIPTION.items()
        if flow not in exclude and flow != "session"
    ]
    if login_flow is None:
        if len(choices) == 1:
            print(f"Only one login flow available: {choices[0].title}")
            login_flow = choices[0].value
        else:
            login_flow = questionary.select(
                "Choose the login flow (How do you going to authenticate?)",
                choices=choices,
                default=current.LOGIN_FLOW if current else "device_code",
            ).unsafe_ask()

    cdf_cluster = questionary.text(
        "Enter the CDF cluster (e.g. westeurope-1)",
        default=current.CDF_CLUSTER if current else "",
        validate=lambda v: True if v.strip() else "CDF cluster cannot be empty",
    ).unsafe_ask()
    cdf_project = questionary.text(
        "Enter the CDF project",
        default=current.CDF_PROJECT if current else "",
        validate=lambda v: True if v.strip() else "CDF project cannot be empty",
    ).unsafe_ask()
    args: dict[str, Any] = (
        current.dump(include_os=False)
        if current and _is_unchanged(current, provider, login_flow, cdf_project, cdf_cluster)  # type: ignore[arg-type]
        else {}
    )
    args.update(
        {"LOGIN_FLOW": login_flow, "CDF_CLUSTER": cdf_cluster, "CDF_PROJECT": cdf_project, "PROVIDER": provider}
    )
    env_vars = EnvironmentVariables(**args)
    idp_tenant_id = env_vars.IDP_TENANT_ID or "IDP_TENANT_ID"
    for field_, value in env_vars.get_required_with_value():
        user_value = get_user_value(field_, value, provider, cdf_cluster, cdf_project, idp_tenant_id)
        setattr(env_vars, field_.name, user_value)
        if field_.name == "IDP_TENANT_ID":
            idp_tenant_id = user_value

    optional_values = env_vars.get_optional_with_value()
    if optional_values:
        print("Additional variables:")
        for field_, value in optional_values:
            print(f"  {field_.name}={value}")
        if questionary.confirm("Do you want to change any of these variables?", default=False).unsafe_ask():
            for field_, value in optional_values:
                user_value = get_user_value(field_, value, provider, cdf_cluster, cdf_project, idp_tenant_id)
                setattr(env_vars, field_.name, user_value)
    return env_vars


def get_user_value(
    field_: Field, value: Any, provider: Provider, cdf_cluster: str, cdf_project: str, idp_tenant_id: str
) -> Any:
    is_secret = field_.metadata["is_secret"]
    display_name = field_.metadata["display_name"]
    hint = ""
    if value:
        default = value
    else:
        default_example = field_.metadata["default_example"]
        example = (
            field_.metadata["example"]
            .get(provider, default_example)
            .format(CDF_CLUSTER=cdf_cluster, CDF_PROJECT=cdf_project, IDP_TENANT_ID=idp_tenant_id)
        )
        if example and not any(marker in example for marker in ("<", ">", " or ")):
            default = example
        else:
            default = ""
            if example:
                hint = f" (e.g. {example})"

    if isinstance(value, list):
        default = ",".join(value)
    elif value is not None and not isinstance(value, str):
        default = str(value)
    prompt = f"Enter the {display_name}{hint}:"
    if is_secret:
        user_value = questionary.password(prompt, default=default).unsafe_ask()
    else:
        user_value = questionary.text(prompt, default=default).unsafe_ask()
    if user_value is None:
        raise typer.Exit(0)
    if field_.type is int:
        try:
            user_value = int(user_value)
        except ValueError:
            print(f"Invalid value: {user_value}. Please enter an integer.")
            return get_user_value(field_, value, provider, cdf_cluster, cdf_project, idp_tenant_id)
    return user_value


def _is_unchanged(
    current: EnvironmentVariables, provider: Provider, login_flow: LoginFlow, cdf_project: str, cdf_cluster: str
) -> bool:
    return (
        current.PROVIDER == provider
        and current.LOGIN_FLOW == login_flow
        and current.CDF_PROJECT == cdf_project
        and current.CDF_CLUSTER == cdf_cluster
    )
