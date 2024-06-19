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

import time
from importlib import resources
from pathlib import Path
from time import sleep
from typing import cast

import questionary
from cognite.client import CogniteClient
from cognite.client.data_classes.capabilities import (
    Capability,
    FunctionsAcl,
    GroupsAcl,
    ProjectsAcl,
)
from cognite.client.data_classes.iam import Group, GroupList, GroupWrite, TokenInspection
from cognite.client.exceptions import CogniteAPIError
from rich import print
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from cognite_toolkit._cdf_tk.constants import COGNITE_MODULES
from cognite_toolkit._cdf_tk.exceptions import (
    AuthorizationError,
    ResourceCreationError,
    ResourceDeleteError,
    ResourceRetrievalError,
    ToolkitFileNotFoundError,
    ToolkitInvalidSettingsError,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    LowSeverityWarning,
    MediumSeverityWarning,
    MissingCapabilityWarning,
)
from cognite_toolkit._cdf_tk.utils import AuthVariables, CDFToolConfig

from ._base import ToolkitCommand


class AuthCommand(ToolkitCommand):
    def execute(
        self,
        ToolGlobals: CDFToolConfig,
        dry_run: bool,
        interactive: bool,
        group_file: str | None,
        update_group: int,
        create_group: str | None,
        verbose: bool,
    ) -> None:
        # TODO: Check if groupsAcl.UPDATE does nothing?
        if create_group is not None and update_group != 0:
            raise ToolkitInvalidSettingsError("--create-group and --update-group are mutually exclusive.")

        if group_file is None:
            template_dir = cast(Path, resources.files("cognite_toolkit"))
            group_path = template_dir.joinpath(
                Path(f"./{COGNITE_MODULES}/common/cdf_auth_readwrite_all/auth/admin.readwrite.group.yaml")
            )
        else:
            group_path = Path(group_file)
        self.check_auth(
            ToolGlobals,
            admin_group_file=group_path,
            update_group=update_group,
            create_group=create_group,
            interactive=interactive,
            dry_run=dry_run,
            verbose=verbose,
        )

    def check_auth(
        self,
        ToolGlobals: CDFToolConfig,
        admin_group_file: Path,
        update_group: int = 0,
        create_group: str | None = None,
        interactive: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        auth_vars = self.initialize_client(ToolGlobals, interactive, verbose)
        if auth_vars.project is None:
            raise AuthorizationError("CDF_PROJECT is not set.")
        cdf_project = auth_vars.project
        token_inspection = self.check_has_any_access(ToolGlobals)

        self.check_has_project_access(token_inspection, cdf_project)

        print(f"[italic]Focusing on current project {cdf_project} only from here on.[/]")

        self.check_has_group_access(ToolGlobals)

        self.check_identity_provider(ToolGlobals, cdf_project)

        try:
            principal_groups = ToolGlobals.client.iam.groups.list()
        except CogniteAPIError as e:
            raise AuthorizationError(f"Unable to retrieve CDF groups.\n{e}")

        if not admin_group_file.exists():
            raise ToolkitFileNotFoundError(f"Group config file does not exist: {admin_group_file.as_posix()}")
        admin_write_group = GroupWrite.load(admin_group_file.read_text())

        print(
            Panel(
                "The Cognite Toolkit expects the following:\n"
                " - The principal used with the Toolkit [yellow]should[/yellow] be connected to "
                "only ONE CDF Group.\n"
                f" - This group [red]must[/red] be named {admin_write_group.name!r}.\n"
                f" - The group {admin_write_group.name!r} [red]must[/red] have capabilities to "
                f"all resources the Toolkit is managing\n"
                " - All he capabilities [yellow]should[/yellow] be scoped to all resources.",
                title="Toolkit Access Group",
                expand=False,
            )
        )
        if interactive:
            Prompt.ask("Press enter key to continue...")

        self.check_principal_groups(principal_groups, admin_write_group)

        missing_capabilities = self.check_has_toolkit_required_capabilities(
            ToolGlobals.client, token_inspection, admin_write_group, cdf_project, admin_write_group.name
        )
        print("---------------------")
        has_added_capabilities = False
        if missing_capabilities:
            if interactive:
                to_create, to_delete = self.upsert_toolkit_group_interactive(principal_groups, admin_write_group)
            else:
                to_create, to_delete = self.upsert_toolkit_group(
                    principal_groups, admin_write_group, update_group, create_group
                )

            created: Group | None = None
            if dry_run:
                if not to_create and not to_delete:
                    print("No groups  would have been made or modified.")
                elif to_create and not to_delete:
                    print(
                        f"Would have created group {to_create.name} with {len(to_create.capabilities or [])} capabilities."
                    )
                elif to_create and to_create:
                    print(
                        f"Would have updated group {to_create.name} with {len(to_create.capabilities or [])} capabilities."
                    )
            elif to_create:
                created = self.upsert_group(
                    ToolGlobals.client, to_create, to_delete, principal_groups, interactive, cdf_project
                )
                has_added_capabilities = True

            must_switch_principal = created and created.source_id not in {group.source_id for group in principal_groups}
            if must_switch_principal and created:
                print(
                    Panel(
                        f"To use the Toolkit, for example, 'cdf-tk deploy', [red]you need to switch[/red] "
                        f"to the principal with source-id {created.source_id!r}.",
                        title="Switch Principal",
                        expand=False,
                    )
                )

        self.check_function_service_status(ToolGlobals.client, dry_run, has_added_capabilities)

    def initialize_client(self, ToolGlobals: CDFToolConfig, interactive: bool, verbose: bool) -> AuthVariables:
        print("[bold]Checking current service principal/application and environment configurations...[/]")
        auth_vars = AuthVariables.from_env()
        if interactive:
            result = auth_vars.from_interactive_with_validation(verbose)
        else:
            result = auth_vars.validate(verbose)
        if result.messages:
            print("\n".join(result.messages))
        print("  [bold green]OK[/]")
        ToolGlobals.initialize_from_auth_variables(auth_vars)
        return auth_vars

    def check_has_any_access(self, ToolGlobals: CDFToolConfig) -> TokenInspection:
        print("Checking basic project configuration...")
        try:
            # Using the token/inspect endpoint to check if the client has access to the project.
            # The response also includes access rights, which can be used to check if the client has the
            # correct access for what you want to do.
            token_inspection = ToolGlobals.client.iam.token.inspect()
            if token_inspection is None or len(token_inspection.capabilities) == 0:
                raise AuthorizationError(
                    "Valid authentication token, but it does not give any access rights."
                    " Check credentials (CDF_CLIENT_ID/CDF_CLIENT_SECRET or CDF_TOKEN)."
                )
            print("  [bold green]OK[/]")
        except Exception:
            raise AuthorizationError(
                "Not a valid authentication token. Check credentials (CDF_CLIENT_ID/CDF_CLIENT_SECRET or CDF_TOKEN)."
                "This could also be due to the service principal/application not having access to any Groups."
            )
        return token_inspection

    def check_has_project_access(self, token_inspection: TokenInspection, cdf_project: str) -> None:
        print("Checking projects that the service principal/application has access to...")
        if len(token_inspection.projects) == 0:
            raise AuthorizationError(
                "The service principal/application configured for this client does not have access to any projects."
            )
        print("\n".join(f"  - {p.url_name}" for p in token_inspection.projects))
        if cdf_project not in {p.url_name for p in token_inspection.projects}:
            raise AuthorizationError(
                f"The service principal/application configured for this client does not have access to the CDF_PROJECT={cdf_project!r}."
            )

    def check_has_group_access(self, ToolGlobals: CDFToolConfig) -> None:
        # Todo rewrite to use the token inspection instead.
        print(
            "Checking basic project and group manipulation access rights "
            "(projectsAcl: LIST, READ and groupsAcl: LIST, READ, CREATE, UPDATE, DELETE)..."
        )
        try:
            ToolGlobals.verify_authorization(
                [
                    ProjectsAcl([ProjectsAcl.Action.List, ProjectsAcl.Action.Read], ProjectsAcl.Scope.All()),
                    GroupsAcl(
                        [
                            GroupsAcl.Action.Read,
                            GroupsAcl.Action.List,
                            GroupsAcl.Action.Create,
                            GroupsAcl.Action.Update,
                            GroupsAcl.Action.Delete,
                        ],
                        GroupsAcl.Scope.All(),
                    ),
                ]
            )
            print("  [bold green]OK[/]")
        except Exception:
            self.warn(
                HighSeverityWarning(
                    "The service principal/application configured for this client "
                    "does not have the basic group write access rights."
                )
            )
            print("Checking basic group read access rights (projectsAcl: LIST, READ and groupsAcl: LIST, READ)...")
            try:
                ToolGlobals.verify_authorization(
                    capabilities=[
                        ProjectsAcl([ProjectsAcl.Action.List, ProjectsAcl.Action.Read], ProjectsAcl.Scope.All()),
                        GroupsAcl([GroupsAcl.Action.Read, GroupsAcl.Action.List], GroupsAcl.Scope.All()),
                    ]
                )
                print("  [bold green]OK[/] - can continue with checks.")
            except Exception:
                raise AuthorizationError(
                    "Unable to continue, the service principal/application configured for this client does not"
                    " have the basic read group access rights."
                )

    def check_identity_provider(self, ToolGlobals: CDFToolConfig, cdf_project: str) -> None:
        print("Checking identity provider settings...")
        project_info = ToolGlobals.client.get(f"/api/v1/projects/{cdf_project}").json()
        oidc = project_info.get("oidcConfiguration", {})
        if "https://login.windows.net" in oidc.get("tokenUrl"):
            tenant_id = oidc.get("tokenUrl").split("/")[-3]
            print(f"  [bold green]OK[/]: Microsoft Entra ID (aka ActiveDirectory) with tenant id ({tenant_id}).")
        elif "auth0.com" in oidc.get("tokenUrl"):
            tenant_id = oidc.get("tokenUrl").split("/")[2].split(".")[0]
            print(f"  [bold green]OK[/] - Auth0 with tenant id ({tenant_id}).")
        else:
            self.warn(MediumSeverityWarning(f"Unknown identity provider {oidc.get('tokenUrl')}"))
        access_claims = [c.get("claimName") for c in oidc.get("accessClaims", {})]
        print(
            f"  Matching on CDF group sourceIds will be done on any of these claims from the identity provider: {access_claims}"
        )

    def check_principal_groups(self, principal_groups: GroupList, admin_group: GroupWrite) -> None:
        print("Checking CDF group memberships for the current client configured...")

        table = Table(title="CDF Group ids, Names, and Source Ids")
        table.add_column("Id", justify="left")
        table.add_column("Name", justify="left")
        table.add_column("Source Id", justify="left")
        admin_group_read = GroupList([])
        for group in principal_groups:
            name = group.name
            if group.name == admin_group.name:
                admin_group_read.append(group)
                name = f"[bold]{group.name}[/]"

            table.add_row(str(group.id), name, group.source_id)
        print(table)

        if len(principal_groups) > 1:
            self.warn(
                LowSeverityWarning(
                    "This service principal/application gets its access rights from more than one CDF group."
                    "           This is not recommended. The group matching the group config file is marked in "
                    "bold above if it is present."
                )
            )
        else:
            print("  [bold green]OK[/] - Only one group is used for this service principal/application.")

        print("---------------------")
        if len(admin_group_read) == 0:
            # No group existing Toolkit group
            return None

        elif len(admin_group_read) > 1:
            self.warn(
                MediumSeverityWarning(
                    f"There are multiple groups with the same name {admin_group.name} in the CDF project."
                    "           It is recommended that this admin (CI/CD) application/service principal "
                    "only is member of one group in the identity provider. Suggest you delete all but one"
                    "           of the groups with the same name."
                )
            )
            # Todo: New feature ask user to cleanup groups with the same name.
            #    Should compare the groups to the admin_group and ask to cleanup the groups
            #    that are not the same.

        # Check for the reuse of Source ID
        unique_source_ids = set(group.source_id for group in principal_groups)
        for source_id in unique_source_ids:
            group_names = [
                group.name
                for group in principal_groups
                if group.source_id == source_id and group.name != admin_group.name
            ]
            if len(group_names) > 1:
                groups_names_str = "\n  - ".join(group_names)
                self.warn(
                    LowSeverityWarning(
                        f"The following groups have the same source id, {source_id}, "
                        f"as the admin group {admin_group.name}: \n{groups_names_str}.\n"
                        "It is recommended that this admin (CI/CD) application/service principal "
                        "is only member of one group in the identity provider."
                    )
                )
        return None

    def check_has_toolkit_required_capabilities(
        self,
        client: CogniteClient,
        token_inspection: TokenInspection,
        admin_group: GroupWrite,
        cdf_project: str,
        group_file_name: str,
    ) -> list[Capability]:
        print(f"\nChecking CDF groups access right against capabilities in {group_file_name} ...")

        missing_capabilities = client.iam.compare_capabilities(
            token_inspection.capabilities,
            admin_group.capabilities or [],
            project=cdf_project,
        )
        if missing_capabilities:
            for s in sorted(map(str, missing_capabilities)):
                self.warn(MissingCapabilityWarning(s))
        else:
            print("  [bold green]OK[/] - All capabilities are present in the CDF project.")
        return missing_capabilities

    def upsert_toolkit_group_interactive(
        self, principal_groups: GroupList, admin_group: GroupWrite
    ) -> tuple[GroupWrite | None, Group | None]:
        new_admin_group = GroupWrite.load(admin_group.dump())
        update_candidates = [group for group in principal_groups if group.name == admin_group.name]
        has_candidates = len(update_candidates) > 0
        update_group: Group | None = None
        if has_candidates and Confirm.ask(
            f"Do you want to update the group with name {admin_group.name!r} with the capabilities "
            "from the group config file?",
            choices=["y", "n"],
        ):
            if len(update_candidates) > 1:
                update_group = questionary.select(
                    "Select the group to update",
                    choices=[
                        {
                            f"{group.id}  - {group.source_id} - {len(group.capabilities or [])} capabilities - {group.metadata!r}": group
                        }
                        for group in update_candidates
                    ],
                ).ask()  # returns value of selection
            elif len(update_candidates) == 1:
                update_group = update_candidates[0]

            if update_group is not None:
                new_admin_group.source_id = update_group.source_id
                return new_admin_group, update_group

        prefix = f"No {admin_group.name} exists. " if not has_candidates else ""
        if not Confirm.ask(
            f"{prefix}Do you want to create a new group for running the toolkit "
            "with the capabilities from the group config file ?",
            choices=["y", "n"],
        ):
            return None, None
        new_source_id = str(
            Prompt.ask("What is the source id for the new group (typically a group id in the identity provider)? ")
        )
        new_admin_group.source_id = new_source_id
        return new_admin_group, None

    def upsert_toolkit_group(
        self, principal_groups: GroupList, admin_group: GroupWrite, update_group: int, create_group: str | None
    ) -> tuple[GroupWrite | None, Group | None]:
        new_admin_group = GroupWrite.load(admin_group.dump())
        if create_group is not None:
            new_admin_group.source_id = create_group
            return new_admin_group, None

        update_candidates = [group for group in principal_groups if group.name == admin_group.name]
        if update_group == 1 and len(update_candidates) > 1:
            group_ids = "         \n - ".join([str(group.id) for group in update_candidates])
            raise AuthorizationError(
                "You have specified --update-group=1.\n"
                "         With multiple groups available, you must use the --update_group=<full-group-i> "
                f"option to specify which group to update. One of the following group ids must be used:\n{group_ids}"
            )
        elif update_group == 1 and len(update_candidates) == 1:
            new_admin_group.source_id = update_candidates[0].source_id
            return new_admin_group, update_candidates[0]
        elif update_group > 1:
            try:
                to_update_group = next(g for g in principal_groups if g.id == update_group)
                new_admin_group.source_id = to_update_group.source_id
                return new_admin_group, to_update_group
            except StopIteration:
                raise ResourceRetrievalError(f"Unable to find --group-id={update_group} in CDF.")
        return None, None

    def upsert_group(
        self,
        client: CogniteClient,
        to_create: GroupWrite,
        to_delete: Group | None,
        principal_groups: GroupList,
        interactive: bool,
        cdf_project: str,
    ) -> Group | None:
        existing_groups = [
            group
            for group in principal_groups
            if group.source_id == to_create.source_id and (to_delete is None or group.id != to_delete.id)
        ]
        if existing_groups:
            existing_str = "\n  - ".join([f"{group.id} - {group.name}" for group in existing_groups])
            self.warn(
                HighSeverityWarning(
                    f"Group with source id {to_create.source_id} already exists in CDF.\n{existing_str}"
                )
            )
            if interactive and not Confirm.ask("Do you want to continue and create a new group?", choices=["y", "n"]):
                return None

        if to_delete:
            existing_capabilities = to_delete.capabilities or []
            new_capabilities = to_create.capabilities or []
            loosing = client.iam.compare_capabilities(new_capabilities, existing_capabilities, project=cdf_project)
            for capability in loosing:
                if len(principal_groups) > 1:
                    self.warn(
                        LowSeverityWarning(
                            f"The capability {capability} may be lost if\n"
                            "           switching to relying on only one group based on "
                            "group config file for access."
                        )
                    )
                else:
                    self.warn(
                        LowSeverityWarning(
                            f"The capability {capability} will be removed in the project if overwritten by group config file."
                        )
                    )
            if (
                loosing
                and interactive
                and not Confirm.ask("Do you want to continue and update the group?", choices=["y", "n"])
            ):
                return None

        action = "create" if to_delete is None else "update"
        try:
            created = client.iam.groups.create(to_create)
        except CogniteAPIError as e:
            raise ResourceCreationError(f"Unable to {action} group {to_create.name}.\n{e}")
        if to_delete:
            try:
                client.iam.groups.delete(to_delete.id)
            except CogniteAPIError as e:
                raise ResourceDeleteError(
                    f"Failed to cleanup old version of the {to_delete.name}.\n{e}\n"
                    f"It is recommended that you manually delete the Group with ID {to_delete.id},"
                    f"such that you don't have a duplicated group in your CDF project."
                )
        print(
            f"  [bold green]OK[/] - {action.capitalize()}d new group {created.name} with {len(created.capabilities or [])} capabilities."
        )
        return created

    def check_function_service_status(self, client: CogniteClient, dry_run: bool, has_added_capabilities: bool) -> None:
        print("Checking function service status...")
        has_function_read_access = self.has_function_rights(client, [FunctionsAcl.Action.Read], has_added_capabilities)
        if not has_function_read_access:
            self.warn(HighSeverityWarning("Cannot check function service status, missing function read access."))
            return None
        try:
            function_status = client.functions.status()
        except CogniteAPIError as e:
            self.warn(HighSeverityWarning(f"Unable to check function service status.\n{e}"))
            return None

        if function_status.status == "requested":
            print("  [bold yellow]INFO:[/] Function service activation is in progress (may take up to 2 hours)...")
        elif dry_run and function_status.status != "activated":
            print(
                "  [bold yellow]INFO:[/] Function service has not been activated, "
                "would have activated (will take up to 2 hours)..."
            )
        elif not dry_run and function_status.status != "activated":
            has_function_write_access = self.has_function_rights(
                client, [FunctionsAcl.Action.Write], has_added_capabilities
            )
            if not has_function_write_access:
                self.warn(HighSeverityWarning("Cannot activate function service, missing function write access."))
                return None
            try:
                client.functions.activate()
            except CogniteAPIError as e:
                self.warn(HighSeverityWarning(f"Unable to activate function service.\n{e}"))
                return None
            print(
                "  [bold green]OK[/] - Function service has been activated. "
                "This may take up to 2 hours to take effect."
            )

        else:
            print("  [bold green]OK[/] - Function service has been activated.")

        return None

    def has_function_rights(
        self, client: CogniteClient, actions: list[FunctionsAcl.Action], has_added_capabilities: bool
    ) -> bool:
        t0 = time.perf_counter()
        while not (
            has_function_access := not client.iam.verify_capabilities(
                FunctionsAcl(actions, FunctionsAcl.Scope.All()),
            )
        ):
            if has_added_capabilities and (time.perf_counter() - t0 < 5.0):
                # Wait for the IAM service to update the capabilities
                sleep(1.0)
            else:
                break
        return has_function_access
