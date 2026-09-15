import os
import sys
from dataclasses import Field
from typing import Any

import questionary
import typer
from questionary import Choice
from rich import print

from cognite_toolkit._cdf_tk.exceptions import AuthenticationError

from .cogidp import SessionProject, fetch_session_user_info
from .data_classes import (
    LOGIN_FLOWS,
    PROVIDERS,
    EnvironmentVariables,
    LoginFlow,
    Provider,
)
from .session_store import StoredSession


def _available_project_names(projects: list[SessionProject]) -> str:
    return ", ".join(sorted(project.name for project in projects))


def _cluster_from_session_project(project: SessionProject) -> str:
    if project.cluster and project.cluster.strip():
        return project.cluster.strip()
    raise AuthenticationError(
        f"CogIdP did not return a cluster for project {project.name!r}. Contact your organization administrator."
    )


def _resolve_named_project(
    project_name: str,
    projects: list[SessionProject],
    *,
    org: str,
) -> tuple[str, str]:
    matched = next((item for item in projects if item.name == project_name), None)
    if matched is None:
        raise AuthenticationError(
            f"Project {project_name!r} is not available for organization {org!r}. "
            f"Available projects: {_available_project_names(projects)}"
        )
    return project_name, _cluster_from_session_project(matched)


def _project_choice_title(project: SessionProject) -> str:
    cluster = project.cluster or "(unknown cluster)"
    title = f"{project.name} — {cluster}"
    if project.is_default:
        title += " [default]"
    return title


def _select_project_from_list(
    projects: list[SessionProject],
    *,
    default_name: str,
) -> SessionProject:
    sorted_projects = sorted(projects, key=lambda item: (item.cluster or "", item.name))
    default_project = next(
        (item for item in sorted_projects if item.name == default_name),
        next((item for item in sorted_projects if item.is_default), sorted_projects[0]),
    )
    selected_name: str = questionary.select(
        "Select the CDF project to use with the toolkit",
        choices=[Choice(title=_project_choice_title(item), value=item.name) for item in sorted_projects],
        default=default_project.name,
    ).unsafe_ask()
    if selected_name is None:
        raise typer.Exit(0)
    return next(item for item in projects if item.name == selected_name)


def resolve_session_cdf_target(
    session: StoredSession,
    *,
    project: str | None = None,
) -> tuple[str, str]:
    """Resolve CDF project and cluster after a CogIdP session login."""
    fallback_project = os.environ.get("CDF_PROJECT", "").strip()
    user_info = fetch_session_user_info(session.org, session.access_token)
    projects = user_info.projects

    if project and project.strip():
        project_name = project.strip()
        if not projects:
            raise AuthenticationError(
                "CogIdP returned no projects; cannot resolve cluster for --project. "
                "Run login interactively or check your organization access."
            )
        return _resolve_named_project(project_name, projects, org=session.org)

    if len(projects) == 1:
        only = projects[0]
        return only.name, _cluster_from_session_project(only)

    if len(projects) > 1:
        if not sys.stdin.isatty():
            raise AuthenticationError(
                "CDF project is required. Pass --project when running without an interactive terminal."
            )
        default_name = (
            fallback_project
            or next(
                (item for item in projects if item.is_default),
                projects[0],
            ).name
        )
        selected = _select_project_from_list(projects, default_name=default_name)
        return selected.name, _cluster_from_session_project(selected)

    return _prompt_cdf_project_and_cluster(fallback_project=fallback_project)


def _prompt_cdf_project_and_cluster(
    *,
    fallback_project: str,
) -> tuple[str, str]:
    if not sys.stdin.isatty():
        raise AuthenticationError(
            "CDF project is required. Pass --project when running without an interactive terminal."
        )
    print("[dim]No projects returned from CogIdP; enter project details manually.[/dim]")
    cdf_project = questionary.text(
        "Enter the CDF project",
        default=fallback_project,
        validate=lambda value: bool(value.strip()) or "CDF project cannot be empty",
    ).unsafe_ask()
    cdf_project = cdf_project.strip()
    fallback_cluster = os.environ.get("CDF_CLUSTER", "").strip()
    cdf_cluster = questionary.text(
        "Enter the CDF cluster (e.g. westeurope-1)",
        default=fallback_cluster,
        validate=lambda value: bool(value.strip()) or "CDF cluster cannot be empty",
    ).unsafe_ask()
    return cdf_project, cdf_cluster.strip()


def prompt_user_environment_variables(
    current: EnvironmentVariables | None = None,
    *,
    login_flow: LoginFlow | None = None,
) -> EnvironmentVariables:
    provider = questionary.select(
        "Choose the provider (Who authenticates you?)",
        choices=[
            Choice(title=f"{provider}: {description}", value=provider)
            for provider, description in PROVIDERS.items()
            if provider != "cdf"
        ],
        default=current.PROVIDER if current else "entra_id",
    ).unsafe_ask()
    exclude = set()
    if provider == "cdf":
        exclude = set(LOGIN_FLOWS) - {"client_credentials"}
    choices = [
        Choice(title=f"{flow}: {description}", value=flow)
        for flow, description in LOGIN_FLOWS.items()
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
