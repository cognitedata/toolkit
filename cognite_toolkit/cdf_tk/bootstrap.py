# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from cognite.client import CogniteClient
from cognite.client.data_classes.capabilities import (
    UserProfilesAcl,
)
from cognite.client.data_classes.iam import Group
from rich import print
from rich.markup import escape
from rich.prompt import Confirm, Prompt
from rich.table import Table

from .utils import CDFToolConfig


@dataclass
class AuthVariables:
    cluster: str | None
    project: str | None
    token: str | None
    client_id: str | None
    client_secret: str | None
    cdf_url: str | None = None
    token_url: str | None = None
    tenant_id: str | None = None
    audience: str | None = None
    scopes: str | None = None
    ok: bool = False
    info: str = ""
    error: bool = False
    warning: bool = False


def get_auth_variables(interactive: bool = False, verbose: bool = False) -> AuthVariables:
    vars = AuthVariables(
        cluster=os.environ.get("CDF_CLUSTER"),
        project=os.environ.get("CDF_PROJECT"),
        token=os.environ.get("CDF_TOKEN"),
        client_id=os.environ.get("IDP_CLIENT_ID"),
        client_secret=os.environ.get("IDP_CLIENT_SECRET"),
        cdf_url=os.environ.get("CDF_URL"),
        tenant_id=os.environ.get("IDP_TENANT_ID"),
        audience=os.environ.get("IDP_AUDIENCE"),
        scopes=os.environ.get("IDP_SCOPES"),
    )
    if interactive:
        if vars.cluster is None or len(vars.cluster) == 0:
            vars.cluster = "westeurope-1"
        vars.cluster = Prompt.ask("CDF project cluster (e.g. [italic]westeurope-1[/])? ", default=vars.cluster)
    if vars.cluster is None or len(vars.cluster) == 0:
        vars.error = True
        vars.info += "  [bold red]ERROR[/]: Environment variable CDF_CLUSTER must be set or use --interactive.\n"
        return vars
    if vars.cluster is not None and len(vars.cluster) > 0:
        if verbose:
            vars.info += f"  CDF_CLUSTER={vars.cluster} is set correctly.\n"
    default_cdf_url = f"https://{vars.cluster}.cognitedata.com"
    default_audience = f"https://{vars.cluster}.cognitedata.com"
    default_scopes = f"https://{vars.cluster}.cognitedata.com/.default"
    if vars.cdf_url is None:
        vars.cdf_url = default_cdf_url
    if vars.audience is None:
        vars.audience = default_audience
    if vars.scopes is None:
        vars.scopes = default_scopes
    if interactive:
        vars.cdf_url = Prompt.ask(
            f"What is your CDF URL (recommended: [italic]{default_cdf_url}[/]) ? ", default=vars.cdf_url
        )
        vars.audience = Prompt.ask(
            f"What is your IDP audience (recommended: [italic]{default_audience}[/])? ", default=vars.audience
        )
        vars.scopes = Prompt.ask(
            f"What are your IDP scopes (recommended: [italic]{default_scopes}[/]) ? ", default=vars.scopes
        )
        vars.project = Prompt.ask("CDF project URL name (e.g. [italic]publicdata[/])? ", default=vars.project)
    if vars.cdf_url != f"https://{vars.cluster}.cognitedata.com":
        vars.warning = True
        vars.info += f"  [bold yellow]WARNING[/]: CDF_URL is set to {vars.cdf_url}, are you sure it shouldn't be https://{vars.cluster}.cognitedata.com?\n"
    elif verbose:
        vars.info += "  CDF_URL is set correctly.\n"
    if vars.project is not None and len(vars.project) > 0:
        if verbose:
            vars.info += f"  CDF_PROJECT={vars.project} is set correctly.\n"
    else:
        vars.error = True
        vars.info += "  [bold red]ERROR[/]: Environment variable CDF_PROJECT must be set or use --interactive.\n"
        return vars
    if vars.audience != f"https://{vars.cluster}.cognitedata.com":
        vars.warning = True
        vars.info += f"  [bold yellow]WARNING[/]: IDP_AUDIENCE is set to {vars.audience}, are you sure it shouldn't be https://{vars.cluster}.cognitedata.com?\n"
    elif verbose:
        vars.info += f"  IDP_AUDIENCE = {vars.audience} is set correctly.\n"
    if vars.scopes != f"https://{vars.cluster}.cognitedata.com/.default":
        vars.warning = True
        vars.info += f"  [bold yellow]WARNING[/]: IDP_SCOPES is set to {vars.scopes}, are you sure it shouldn't be https://{vars.cluster}.cognitedata.com/.default?\n"
    elif verbose:
        vars.info += f"  IDP_SCOPES = {vars.scopes} is set correctly.\n"
    if interactive:
        token = False
        if vars.token is None or len(vars.token) == 0:
            token = Confirm.ask(
                "Do you have client id and client secret for a service principal/application? ",
                choices=["y", "n"],
            )
            if not token:
                vars.token = Prompt.ask("OAuth2 token (CDF_TOKEN)? ", password=True)
        else:
            new_token = Prompt.ask(
                "You have set an OAuth2 token (CDF_TOKEN), change it (press ENTER to keep current) ? ",
                password=True,
                default="",
            )
            if len(new_token) > 0:
                vars.token = new_token
            else:
                print("  Keeping existing token.")
        if token:
            azure = Confirm.ask(
                "Do you have Microsoft Entra ID/ActiveDirectory as your identity provider ?", choices=["y", "n"]
            )
            name_of_principal = "Service principal/application"
            if azure:
                vars.tenant_id = Prompt.ask(
                    "What is your Entra tenant id (e.g. [italic]12345678-1234-1234-1234-123456789012[/])? ",
                    default=vars.tenant_id,
                )
                name_of_principal = "Application"
                vars.token_url = f"https://login.microsoftonline.com/{vars.tenant_id}/oauth2/v2.0/token"
            else:
                vars.token_url = Prompt.ask(
                    "What is your identity provider token endpoint (e.g. [italic]https://myidp.com/oauth2/token[/])? "
                )
            vars.client_id = Prompt.ask(f"{name_of_principal} client id (CDF_CLIENT_ID)? ", default=vars.client_id)
            if vars.client_secret is not None and len(vars.client_secret) > 0:
                new_secret = Prompt.ask(
                    "You have set a client secret (CDF_CLIENT_SECRET), change it (press ENTER to keep current) ? ",
                    password=True,
                    default="",
                )
                if len(new_secret) > 0:
                    vars.client_secret = new_secret
                else:
                    print("  Keeping existing client secret.")
            else:
                vars.client_secret = Prompt.ask(
                    f"{name_of_principal} client secret (CDF_CLIENT_SECRET)",
                    password=True,
                )
        if (
            vars.client_id is None
            or len(vars.client_id) == 0
            or vars.client_secret is None
            or len(vars.client_secret) == 0
        ):
            if vars.token is None or len(vars.token) == 0:
                vars.error = True
                vars.info += "  [bold red]ERROR[/]: Environment variables IDP_CLIENT_ID and IDP_CLIENT_SECRET (or CDF_TOKEN) must be set.\n"
                return vars
            elif verbose:
                vars.info += "  CDF_TOKEN is set, using it as Bearer token for authorization.\n"
        elif verbose:
            vars.info += "  IDP_CLIENT_ID and IDP_CLIENT_SECRET are set correctly.\n"
    if interactive:
        # Write .env file
        if Path(".env").exists():
            print(
                "[bold yellow]WARNING[/]: .env file already exists and values have been retrieved from it. It will be overwritten."
            )
        write = Confirm.ask(
            "Do you want to save these to .env file for next time ? ",
            choices=["y", "n"],
        )
        if write:
            with open(Path(".env"), "w") as f:
                f.write("# .env file generated by cognite-toolkit\n")
                f.write("CDF_CLUSTER=" + vars.cluster + "\n")
                f.write("CDF_PROJECT=" + vars.project + "\n")
                if vars.token is not None and len(vars.token) > 0:
                    f.write("# When using a token, the IDP variables are not needed, so they are not included.\n")
                    f.write("CDF_TOKEN=" + vars.token + "\n")
                else:
                    f.write("IDP_CLIENT_ID=" + (vars.client_id or "") + "\n")
                    f.write("IDP_CLIENT_SECRET=" + (vars.client_secret or "") + "\n")
                    if vars.tenant_id is not None and len(vars.tenant_id) > 0:
                        f.write("IDP_TENANT_ID=" + vars.tenant_id + "\n")
                    f.write("IDP_TOKEN_URL=" + (vars.token_url or "") + "\n")
                f.write("# The below variables don't have to be set if you have just accepted the defaults.\n")
                f.write("# They are automatically constructed unless they are set.\n")
                f.write("CDF_URL=" + vars.cdf_url + "\n")
                if vars.token is None:
                    f.write("IDP_AUDIENCE=" + vars.audience + "\n")
                    f.write("IDP_SCOPES=" + vars.scopes + "\n")
    return vars


def check_auth(
    ToolGlobals: CDFToolConfig,
    group_file: Path,
    auth_vars: AuthVariables | None = None,
    update_group: int = 0,
    create_group: str | None = None,
    interactive: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> CogniteClient | None:
    print("[bold]Checking current service principal/application and environment configurations...[/]")
    if auth_vars is None:
        auth_vars = get_auth_variables(verbose=verbose, interactive=interactive)
    if auth_vars.error:
        print(auth_vars.info)
        ToolGlobals.failed = True
        return None
    if auth_vars.warning:
        print(auth_vars.info)
    else:
        print("  [bold green]OK[/]")
    print("Checking basic project configuration...")
    try:
        # Using the token/inspect endpoint to check if the client has access to the project.
        # The response also includes access rights, which can be used to check if the client has the
        # correct access for what you want to do.
        resp = ToolGlobals.client.iam.token.inspect()
        if resp is None or len(resp.capabilities) == 0:
            print(
                "  [bold red]ERROR[/]: Valid authentication token, but it does not give any access rights. Check credentials (CDF_CLIENT_ID/CDF_CLIENT_SECRET or CDF_TOKEN)."
            )
            ToolGlobals.failed = True
            return None
        print("  [bold green]OK[/]")
    except Exception:
        print(
            "  [bold red]ERROR[/]: Not a valid authentication token. Check credentials (CDF_CLIENT_ID/CDF_CLIENT_SECRET or CDF_TOKEN)."
        )
        ToolGlobals.failed = True
        return None
    try:
        print("Checking projects that the service principal/application has access to...")
        if len(resp.projects) == 0:
            print(
                "  [bold red]ERROR[/]: The service principal/application configured for this client does not have access to any projects."
            )
            ToolGlobals.failed = True
            return None
        projects = ""
        projects = projects.join(f"  - {p.url_name}\n" for p in resp.projects)
        print(projects[0:-1])
    except Exception as e:
        print(f"  [bold red]ERROR[/]: Failed to process project information from inspect()\n{e}")
        ToolGlobals.failed = True
        return None
    print(f"[italic]Focusing on current project {auth_vars.project} only from here on.[/]")
    print(
        "Checking basic project and group manipulation access rights (projectsAcl: LIST, READ and groupsAcl: LIST, READ, CREATE, UPDATE, DELETE)..."
    )
    try:
        ToolGlobals.verify_client(
            capabilities={
                "projectsAcl": ["LIST", "READ"],
                "groupsAcl": ["LIST", "READ", "CREATE", "UPDATE", "DELETE"],
            }
        )
        print("  [bold green]OK[/]")
    except Exception:
        print(
            "  [bold yellow]WARNING[/]: The service principal/application configured for this client does not have the basic group write access rights."
        )
        print("Checking basic group read access rights (projectsAcl: LIST, READ and groupsAcl: LIST, READ)...")
        try:
            ToolGlobals.verify_client(
                capabilities={
                    "projectsAcl": ["LIST", "READ"],
                    "groupsAcl": ["LIST", "READ"],
                }
            )
            print("  [bold green]OK[/] - can continue with checks.")
        except Exception:
            print(
                "    [bold red]ERROR[/]: Unable to continue, the service principal/application configured for this client does not have the basic read group access rights."
            )
            ToolGlobals.failed = True
            return None
    project_info = ToolGlobals.client.get(f"/api/v1/projects/{auth_vars.project}").json()
    print("Checking identity provider settings...")
    oidc = project_info.get("oidcConfiguration", {})
    tenant_id = None
    if "https://login.windows.net" in oidc.get("tokenUrl"):
        tenant_id = oidc.get("tokenUrl").split("/")[-3]
        print(f"  [bold green]OK[/]: Microsoft Entra ID (aka ActiveDirectory) with tenant id ({tenant_id}).")
    elif "auth0.com" in oidc.get("tokenUrl"):
        tenant_id = oidc.get("tokenUrl").split("/")[2].split(".")[0]
        print(f"  [bold green]OK[/] - Auth0 with tenant id ({tenant_id}).")
    else:
        print(f"  [bold yellow]WARNING[/]: Unknown identity provider {oidc.get('tokenUrl')}")
    accessClaims = [c.get("claimName") for c in oidc.get("accessClaims", {})]
    print(
        f"  Matching on CDF group sourceIds will be done on any of these claims from the identity provider: {accessClaims}"
    )
    print("Checking CDF group memberships for the current client configured...")
    try:
        groups = ToolGlobals.client.iam.groups.list().data
    except Exception:
        print("  [bold red]ERROR[/]: Unable to retrieve CDF groups.")
        ToolGlobals.failed = True
        return None
    if group_file.exists():
        file_text = group_file.read_text()
    else:
        raise FileNotFoundError(f"Group config file does not exist: {group_file.as_posix()}")
    read_write = Group.load(file_text)
    tbl = Table(title="CDF Group ids, Names, and Source Ids")
    tbl.add_column("Id", justify="left")
    tbl.add_column("Name", justify="left")
    tbl.add_column("Source Id", justify="left")
    matched_group_source_id = None
    matched_group_id = 0
    for g in groups:
        if len(groups) > 1 and g.name == read_write.name:
            matched_group_source_id = g.source_id
            matched_group_id = g.id
            tbl.add_row(str(g.id), "[bold]" + g.name + "[/]", g.source_id)
        else:
            tbl.add_row(str(g.id), g.name, g.source_id)
    multiple_groups_with_source_id = 0
    for g in groups:
        if g.source_id == matched_group_source_id:
            multiple_groups_with_source_id += 1
    print(tbl)
    if len(groups) > 1:
        print(
            "  [bold yellow]WARNING[/]: This service principal/application gets its access rights from more than one CDF group."
        )
        print(
            "           This is not recommended. The group matching the group config file is marked in bold above if it is present."
        )
        if update_group == 1:
            print(
                "  [bold red]ERROR[/]: You have specified --update-group=1.\n"
                + "         With multiple groups available, you must use the --update_group=<full-group-i> option to specify which group to update."
            )
            ToolGlobals.failed = True
            return None
    else:
        print("  [bold green]OK[/] - Only one group is used for this service principal/application.")
    print("---------------------")
    if matched_group_source_id is not None:
        print("[bold green]RECOMMENDATION[/]:")
        print(f"  You have {multiple_groups_with_source_id} groups with source id {matched_group_source_id},")
        print(
            f"  which is the same source id as the [italic]{escape(read_write.name)}[/] group in the group config file."
        )
        print(
            "  It is recommended that this admin (CI/CD) application/service principal only is member of one group in the identity provider."
        )
        print(
            "  This group's id should be configured as the [italic]readwrite_source_id[/] for the common/cdf_auth_readwrite_all module."
        )
    print(f"\nChecking CDF groups access right against capabilities in {group_file.name} ...")

    diff = ToolGlobals.client.iam.compare_capabilities(
        resp.capabilities,
        read_write.capabilities or [],
        project=auth_vars.project,
    )
    if len(diff) > 0:
        diff_list: list[str] = []
        for d in diff:
            diff_list.append(str(d))
        for s in sorted(diff_list):
            print(f"  [bold yellow]WARNING[/]: The capability {s} is not present in the CDF project.")
    else:
        print("  [bold green]OK[/] - All capabilities are present in the CDF project.")
    # Flatten out into a list of acls in the existing project
    existing_cap_list = [c.capability for c in resp.capabilities]
    if len(groups) > 1:
        print(
            "  [bold yellow]WARNING[/]: This service principal/application gets its access rights from more than one CDF group."
        )
    print("---------------------")
    if len(groups) > 1 and update_group > 1:
        print(f"Checking group config file against capabilities only from the group {update_group}...")
        for g in groups:
            if g.id == update_group:
                existing_cap_list = g.capabilities
                break
    else:
        if len(groups) > 1:
            print("Checking group config file against capabilities across [bold]ALL[/] groups...")
        else:
            print("Checking group config file against capabilities in the group...")

    loosing = ToolGlobals.client.iam.compare_capabilities(
        existing_cap_list,
        resp.capabilities,
        project=auth_vars.project,
    )
    loosing = [l for l in loosing if type(l) is not UserProfilesAcl]  # noqa: E741
    if len(loosing) > 0:
        for d in loosing:
            if len(groups) > 1:
                print(
                    f"  [bold yellow]WARNING[/]: The capability {d} may be lost if\n"
                    + "           switching to relying on only one group based on group config file for access."
                )
            else:
                print(
                    f"  [bold yellow]WARNING[/]: The capability {d} will be removed in the project if overwritten by group config file."
                )
    else:
        print("  [bold green]OK[/] - All capabilities from the CDF project are also present in the group config file.")
    print("---------------------")
    if interactive and matched_group_id != 0:
        push_group = Confirm.ask(
            f"Do you want to update the group with id {matched_group_id} and name {read_write.name} with the capabilities from {group_file.as_posix()} ?",
            choices=["y", "n"],
        )
        if push_group:
            update_group = matched_group_id
    elif interactive:
        push_group = Confirm.ask(
            "Do you want to create a new group with the capabilities from the group config file ?",
            choices=["y", "n"],
        )
        if push_group:
            create_group = Prompt.ask(
                "What is the source id for the new group (typically a group id in the identity provider)? "
            )
    if len(groups) == 1 and update_group == 1:
        update_group = groups[0].id
    elif not interactive and matched_group_id != 0 and update_group == 1:
        update_group = matched_group_id
    if update_group > 1 or create_group is not None:
        if update_group > 0:
            print(f"Updating group {update_group}...")
            for g in groups:
                if g.id == update_group:
                    group = g
                    break
            if group is None:
                print(f"  [bold red]ERROR[/]: Unable to find --group-id={update_group} in CDF.")
                ToolGlobals.failed = True
                return None
            read_write.name = group.name
            read_write.source_id = group.source_id
            read_write.metadata = group.metadata
        else:
            print(f"Creating new group based on {group_file.as_posix()}...")
            read_write.source_id = create_group
        try:
            if not dry_run:
                new = ToolGlobals.client.iam.groups.create(read_write)
                new = cast(Group, new)  # Missing overload in .create method.
                print(
                    f"  [bold green]OK[/] - Created new group {new.id} with {len(read_write.capabilities or [])} capabilities."
                )
            else:
                print(
                    f"  [bold green]OK[/] - Would have created new group with {len(read_write.capabilities or [])} capabilities."
                )
        except Exception as e:
            print(f"  [bold red]ERROR[/]: Unable to create new group {read_write.name}.\n{e}")
            ToolGlobals.failed = True
            return None
        if update_group:
            try:
                if not dry_run:
                    ToolGlobals.client.iam.groups.delete(update_group)
                    print(f"  [bold green]OK[/] - Deleted old group {update_group}.")
                else:
                    print(f"  [bold green]OK[/] - Would have deleted old group {update_group}.")
            except Exception as e:
                print(f"  [bold red]ERROR[/]: Unable to delete old group {update_group}.\n{e}")
                ToolGlobals.failed = True
                return None
    print("Checking function service status...")
    function_status = ToolGlobals.client.functions.status()
    if function_status.status != "activated":
        if function_status.status == "requested":
            print("  [bold yellow]INFO:[/] Function service activation is in progress (may take up to 2 hours)...")
        else:
            if not dry_run:
                print(
                    "  [bold yellow]INFO:[/] Function service has not been activated, activating now, this may take up to 2 hours..."
                )
                ToolGlobals.client.functions.activate()
            else:
                print(
                    "  [bold yellow]INFO:[/] Function service has not been activated, would have activated (will take up to 2 hours)..."
                )
    else:
        print("  [bold green]OK[/] - Function service has been activated.")
    return None
