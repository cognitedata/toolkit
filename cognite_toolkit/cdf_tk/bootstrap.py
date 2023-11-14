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
from pathlib import Path

import yaml
from cognite.client import CogniteClient
from cognite.client.data_classes.capabilities import (
    ProjectCapability,
    ProjectCapabilityList,
    ProjectsScope,
    UserProfilesAcl,
)
from cognite.client.data_classes.iam import Group
from rich import print
from rich.table import Table

from .utils import CDFToolConfig


def check_auth(
    ToolGlobals: CDFToolConfig,
    group_file: str | None = None,
    update_group: int = 0,
    create_group: str | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> CogniteClient:
    cluster = ToolGlobals.environ(
        "CDF_CLUSTER",
        None,
        fail=False,
    )
    project = ToolGlobals.environ(
        "CDF_PROJECT",
        None,
        fail=False,
    )
    token = ToolGlobals.environ(
        "CDF_TOKEN",
        None,
        fail=False,
    )
    client_id = ToolGlobals.environ(
        "IDP_CLIENT_ID",
        None,
        fail=False,
    )
    client_secret = ToolGlobals.environ(
        "IDP_CLIENT_SECRET",
        None,
        fail=False,
    )
    print("[bold]Checking current service principal/application...[/]")
    if cluster is None or len(cluster) == 0:
        print("  [bold red]ERROR:[/]Environment variable CDF_CLUSTER must be set.")
        ToolGlobals.failed = True
        return
    if verbose:
        print(f"  CDF_CLUSTER={cluster} is set correctly.")
    if project is None or len(project) == 0:
        print("  [bold red]ERROR:[/]Environment variable CDF_PROJECT must be set.")
        ToolGlobals.failed = True
        return
    if verbose:
        print(f"  CDF_PROJECT={project} is set correctly.")
    if token is not None and len(token) > 0:
        print("  Env variable CDF_TOKEN is set, using it as Bearer token for authorization.")
    else:
        if verbose:
            print("  CDF_TOKEN is not set, expecting IDP_CLIENT_ID and IDP_CLIENT_SECRET...")
        if client_id is None or len(client_id) == 0:
            print("  [bold red]ERROR:[/]Environment variable IDP_CLIENT_ID must be set.")
            ToolGlobals.failed = True
            return
        if client_secret is None or len(client_secret) == 0:
            print("  [bold red]ERROR:[/]Environment variable IDP_CLIENT_SECRET must be set.")
            ToolGlobals.failed = True
            return
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
            return
        print("  [bold green]OK[/]")
    except Exception:
        print(
            "  [bold red]ERROR[/]: Not a valid authentication token. Check credentials (CDF_CLIENT_ID/CDF_CLIENT_SECRET or CDF_TOKEN)."
        )
        ToolGlobals.failed = True
        return
    print("Checking environment variables...")
    ok = True
    if os.environ.get("CDF_URL") is not None and os.environ.get("CDF_URL") != f"https://{cluster}.cognitedata.com":
        print(
            f"  [bold yellow]WARNING[/]: CDF_URL is set to {os.environ.get('CDF_URL')}, are you sure it shouldn't be https://{cluster}.cognitedata.com?"
        )
        ok = False
    elif verbose:
        print("  CDF_URL is set correctly.")
    if (
        os.environ.get("IDP_AUDIENCE") is not None
        and os.environ.get("IDP_AUDIENCE") != f"https://{cluster}.cognitedata.com"
    ):
        print(
            f"  [bold yellow]WARNING[/]: IDP_AUDIENCE is set to {os.environ.get('IDP_AUDIENCE')}, are you sure it shouldn't be https://{cluster}.cognitedata.com?"
        )
        ok = False
    elif verbose:
        print("  IDP_AUDIENCE is set correctly.")
    if (
        os.environ.get("IDP_SCOPES") is not None
        and os.environ.get("IDP_SCOPES") != f"https://{cluster}.cognitedata.com/.default"
    ):
        print(
            f"  [bold yellow]WARNING[/]: IDP_SCOPES is set to {os.environ.get('IDP_SCOPES')}, are you sure it shouldn't be https://{cluster}.cognitedata.com/.default?"
        )
        ok = False
    elif verbose:
        print("  IDP_SCOPES is set correctly.")
    if not ok:
        print(
            "  One or more environment variable checks failed. Should you unset these variables and let them be set default or change them?"
        )
    else:
        print("  [bold green]OK[/]")

    try:
        print("Checking projects that the service principal/application has access to...")
        if len(resp.projects) == 0:
            print(
                "  [bold red]ERROR[/]: The service principal/application configured for this client does not have access to any projects."
            )
            ToolGlobals.failed = True
            return
        projects = ""
        projects = projects.join(f"  - {p.url_name}\n" for p in resp.projects)
        print(projects[0:-1])
    except Exception as e:
        print(f"  [bold red]ERROR[/]: Failed to process project information from inspect()\n{e}")
        ToolGlobals.failed = True
        return
    print(f"[italic]Focusing on project {project} only from here on.[/]")
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
            return
    project_info = ToolGlobals.client.get(f"/api/v1/projects/{project}").json()
    print("Checking identity provider settings...")
    oidc = project_info.get("oidcConfiguration", {})
    tenant_id = None
    if "https://login.windows.net" in oidc.get("tokenUrl"):
        tenant_id = oidc.get("tokenUrl").split("/")[-3]
        print(f"  [bold green]OK[/]: Azure Entra (aka ActiveDirectory) with tenant id ({tenant_id}).")
    elif "auth0.com" in oidc.get("tokenUrl"):
        tenant_id = oidc.get("tokenUrl").split("/")[2].split(".")[0]
        print(f"  [bold green]OK[/] - Auth0 with tenant id ({tenant_id}).")
    else:
        print(f"  [bold yellow]WARNING[/]: Unknown identity provider {oidc.get('tokenUrl')}")
    accessClaims = [c.get("claimName") for c in oidc.get("accessClaims", {})]
    print(
        f"  Matching on CDF group sourceIds will be done on any of these claims from the identity provider: {accessClaims}"
    )
    print("Checking CDF groups...")
    try:
        groups = ToolGlobals.client.iam.groups.list().data
    except Exception:
        print("  [bold red]ERROR[/]: Unable to retrieve CDF groups.")
        ToolGlobals.failed = True
        return
    tbl = Table(title="CDF Group ids, Names, and Source Ids")
    tbl.add_column("Id", justify="left")
    tbl.add_column("Name", justify="left")
    tbl.add_column("Source Id", justify="left")
    for g in groups:
        tbl.add_row(str(g.id), g.name, g.source_id)
    print(tbl)
    if len(groups) > 1:
        print(
            "  [bold yellow]WARNING[/]: This service principal/application gets its access rights from more than one CDF group."
        )
        print("           This is not recommended.")
        if update_group == 1:
            print(
                "  [bold red]ERROR[/]: You have specified --update-group=1.\n"
                + "         With multiple groups available, you must use the --update_group=<full-group-i> option to specify which group to update."
            )
            ToolGlobals.failed = True
            return
    else:
        print("  [bold green]OK[/] - Only one group is used for this service principal/application.")
    print("---------------------")
    print(f"\nChecking CDF groups access right against capabilities in {group_file} ...")
    read_write = Group.load(
        yaml.load(
            Path(f"{Path(__file__).parent.parent.as_posix()}{group_file}").read_text(),
            Loader=yaml.Loader,
        )
    )

    diff = resp.capabilities.compare(read_write.capabilities, project=project)
    if len(diff) > 0:
        for d in diff:
            print(f"  [bold yellow]WARNING[/]: The capability {d} is not present in the CDF project.")
    else:
        print("  [bold green]OK[/] - All capabilities are present in the CDF project.")
    # Flatten out into a list of acls in the existing project
    existing_cap_list = [c.capability for c in resp.capabilities.data]
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
    # Create a list of capabilities from the flattened list of acls in the group file.
    _ = ProjectCapabilityList(
        [ProjectCapability(pc, project_scope=ProjectsScope([project])) for pc in read_write.capabilities]
    )

    loosing = ToolGlobals.client.iam.compare_capabilities(resp.capabilities, existing_cap_list)
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
    if len(groups) == 1 and update_group == 1:
        update_group = groups[0].id
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
                return
            read_write.name = group.name
            read_write.source_id = group.source_id
            read_write.metadata = group.metadata
        else:
            print(f"Creating new group based on {group_file}...")
            read_write.source_id = create_group
        try:
            if not dry_run:
                new = ToolGlobals.client.iam.groups.create(read_write)
                print(
                    f"  [bold green]OK[/] - Created new group {new.id} with {len(read_write.capabilities)} capabilities."
                )
            else:
                print(
                    f"  [bold green]OK[/] - Would have created new group with {len(read_write.capabilities)} capabilities."
                )
        except Exception as e:
            print(f"  [bold red]ERROR[/]: Unable to create new group {read_write.name}.\n{e}")
            ToolGlobals.failed = True
            return
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
                return
