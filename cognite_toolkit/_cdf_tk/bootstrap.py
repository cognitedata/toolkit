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

from .utils import AuthVariables, CDFToolConfig


def check_auth(
    ToolGlobals: CDFToolConfig,
    group_file: Path,
    update_group: int = 0,
    create_group: str | None = None,
    interactive: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> CogniteClient | None:
    print("[bold]Checking current service principal/application and environment configurations...[/]")
    auth_vars = AuthVariables.from_env()
    if interactive:
        result = auth_vars.from_interactive_with_validation(verbose)
    else:
        result = auth_vars.validate(verbose)
    if result.messages:
        print("\n".join(result.messages))
    if result.status == "error":
        ToolGlobals.failed = True
        return None
    print("  [bold green]OK[/]")
    if not ToolGlobals.initialize_from_auth_variables(auth_vars):
        ToolGlobals.failed = True
        return None
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
