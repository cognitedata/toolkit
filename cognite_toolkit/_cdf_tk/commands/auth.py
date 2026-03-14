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


import itertools
import shutil
import time
import urllib.parse
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from typing import Literal

import questionary
from cognite.client.exceptions import CogniteAPIError
from rich import print
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table

from cognite_toolkit._cdf_tk import cruds
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AllScope,
    AssetsAcl,
    FunctionsAcl,
    GroupCapability,
    GroupRequest,
    GroupResponse,
    GroupsAcl,
    ProjectsAcl,
    RelationshipsAcl,
)
from cognite_toolkit._cdf_tk.client.resource_classes.group.acls import AclType
from cognite_toolkit._cdf_tk.client.resource_classes.token import FlatCapabilities, InspectResponse
from cognite_toolkit._cdf_tk.constants import (
    HINT_LEAD_TEXT,
    TOOLKIT_DEMO_GROUP_NAME,
    TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME,
)
from cognite_toolkit._cdf_tk.cruds import AssetCRUD, RelationshipCRUD
from cognite_toolkit._cdf_tk.exceptions import (
    AuthenticationError,
    AuthorizationError,
    ResourceCreationError,
    ResourceDeleteError,
    ToolkitMissingValueError,
)
from cognite_toolkit._cdf_tk.tk_warnings import (
    HighSeverityWarning,
    LowSeverityWarning,
    MediumSeverityWarning,
    MissingCapabilityWarning,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables, prompt_user_environment_variables

from ._base import ToolkitCommand


@dataclass
class VerifyAuthResult:
    toolkit_group_id: int | None = None
    function_status: str | None = None


class AuthCommand(ToolkitCommand):
    def init(self) -> None:
        env_vars: EnvironmentVariables | None = None
        try:
            env_vars = EnvironmentVariables.create_from_environment()
        except ToolkitMissingValueError:
            ...

        ask_user = True
        if env_vars and not env_vars.get_missing_vars():
            print("Auth variables are already set.")
            ask_user = questionary.confirm("Do you want to reconfigure the auth variables?", default=False).unsafe_ask()

        if ask_user or not env_vars:
            env_vars = prompt_user_environment_variables(env_vars)
            self._store_dotenv(env_vars)

        client = env_vars.get_client()
        try:
            client.tool.token.inspect()
        except ToolkitAPIError as e:
            raise AuthenticationError(f"Unable to verify the credentials.\n{e}")

        print("[green]The credentials are valid.[/green]")

    def _store_dotenv(self, env_vars: EnvironmentVariables) -> None:
        new_env_file = env_vars.create_dotenv_file()
        if Path(".env").exists():
            existing = Path(".env").read_text(encoding="utf-8")
            if existing == new_env_file:
                print("Identical '.env' file already exist.")
                return None
            self.warn(MediumSeverityWarning("'.env' file already exists"))
            filename = next(f"backup_{no}.env" for no in itertools.count() if not Path(f"backup_{no}.env").exists())

            if questionary.confirm(
                f"Do you want to overwrite the existing '.env' file? The existing will be renamed to {filename}",
                default=False,
            ).unsafe_ask():
                shutil.move(".env", filename)
                Path(".env").write_text(new_env_file, encoding="utf-8")
        elif questionary.confirm("Do you want to save these to .env file for next time?", default=True).unsafe_ask():
            Path(".env").write_text(new_env_file, encoding="utf-8")

    def verify(
        self,
        client: ToolkitClient,
        dry_run: bool,
        no_prompt: bool = False,
        demo_principal: str | None = None,
    ) -> VerifyAuthResult:
        """Authorization verification for the Toolkit.

        Args:
            client: The Toolkit client.
            dry_run: If the verification should be run in dry-run mode.
            no_prompt: If the verification should be run without any prompts.
            demo_principal: This is used for demo purposes. If passed, a different group name will be used
                to create the Toolkit group. This is group is intended to be deleted after the demo.

        Returns:
            VerifyAuthResult: The result of the verification.
        """

        is_interactive = not no_prompt
        is_demo = demo_principal is not None
        if client.config.project is None:
            raise AuthorizationError("CDF_PROJECT is not set.")
        cdf_project = client.config.project
        inspect_response = self.check_has_any_access(client)

        self.check_has_project_access(inspect_response, cdf_project)

        print(f"[italic]Focusing on current project {cdf_project} only from here on.[/]")

        self.check_has_group_access(client)

        self.check_identity_provider(client, cdf_project)

        try:
            user_groups = client.tool.groups.list(all_groups=False)
        except ToolkitAPIError as e:
            raise AuthorizationError(f"Unable to retrieve CDF groups.\n{e}")

        if not user_groups:
            raise AuthorizationError("The current user is not member of any groups in the CDF project.")

        data_modeling_status = client.project.status().this_project.data_modeling_status
        required_acls, resource_names_by_acl_type = self._get_required_acls(client, data_modeling_status)
        toolkit_group = self._create_toolkit_group(required_acls, demo_principal)

        if not is_demo:
            print(
                Panel(
                    "The Cognite Toolkit expects the following:\n"
                    " - The principal used with the Toolkit [yellow]should[/yellow] be connected to "
                    "only ONE CDF Group.\n"
                    f" - This group [red]must[/red] be named {toolkit_group.name!r}.\n"
                    f" - The group {toolkit_group.name!r} [red]must[/red] have capabilities to "
                    f"all resources the Toolkit is managing\n"
                    " - All the capabilities [yellow]should[/yellow] be scoped to all resources.",
                    title="Toolkit Access Group",
                    expand=False,
                )
            )
            if is_interactive:
                Prompt.ask("Press enter key to continue...")

        all_groups = client.tool.groups.list(all_groups=True)

        is_user_in_toolkit_group = any(group.name == toolkit_group.name for group in user_groups)
        is_toolkit_group_existing = any(group.name == toolkit_group.name for group in all_groups)

        print(f"Checking current client is member of the {toolkit_group.name!r} group...")
        has_added_capabilities = False
        cdf_toolkit_group: GroupResponse | None
        if is_user_in_toolkit_group:
            print(f"  [bold green]OK[/] - The current client is member of the {toolkit_group.name!r} group.")
            cdf_toolkit_group = next(group for group in user_groups if group.name == toolkit_group.name)

            missing_capabilities = self._check_missing_capabilities(
                cdf_toolkit_group, toolkit_group, resource_names_by_acl_type, cdf_project, is_interactive
            )
            if (
                is_interactive
                and missing_capabilities
                and questionary.confirm("Do you want to update the group with the missing capabilities?").unsafe_ask()
            ) or is_demo:
                has_added_capabilities = self._update_missing_capabilities(
                    client, cdf_toolkit_group, missing_capabilities, dry_run, cdf_project, data_modeling_status
                )
        elif is_toolkit_group_existing:  # and not is_user_in_toolkit_group
            self.warn(MediumSeverityWarning(f"The current client is not member of the {toolkit_group.name!r} group."))
            print(f"Checking if the group {toolkit_group.name!r} has the required capabilities...")
            cdf_toolkit_group = next(group for group in all_groups if group.name == toolkit_group.name)
            missing_capabilities = self._check_missing_capabilities(
                cdf_toolkit_group, toolkit_group, resource_names_by_acl_type, cdf_project, is_interactive
            )
            if (
                is_interactive
                and missing_capabilities
                and questionary.confirm("Do you want to update the group with the missing capabilities?").unsafe_ask()
            ):
                self._update_missing_capabilities(
                    client, cdf_toolkit_group, missing_capabilities, dry_run, cdf_project, data_modeling_status
                )
        elif is_demo:
            cdf_toolkit_group = self._create_toolkit_group_in_cdf(client, toolkit_group)
        else:
            print(f"Group {toolkit_group.name!r} does not exist in the CDF project.")
            cdf_toolkit_group = self._create_toolkit_group_in_cdf_interactive(
                client, toolkit_group, all_groups, is_interactive, dry_run
            )
        if cdf_toolkit_group is None:
            return VerifyAuthResult()

        if not is_demo and not is_user_in_toolkit_group:
            print(
                Panel(
                    "To use the Toolkit, for example, 'cdf deploy', [red]you need[/red] to make sure to use a service principal "
                    f"that is a member of the group with object id {cdf_toolkit_group.source_id!r}.",
                    title="Service Principal group membership",
                    expand=False,
                )
            )
            return VerifyAuthResult(function_status=None, toolkit_group_id=cdf_toolkit_group.id)

        if not is_demo:
            self.check_count_group_memberships(user_groups)

            self.check_source_id_usage(all_groups, cdf_toolkit_group)

            if extra := self.check_duplicated_names(all_groups, cdf_toolkit_group):
                if (
                    is_interactive
                    and questionary.confirm("Do you want to delete the extra groups?", default=True).unsafe_ask()
                ):
                    try:
                        client.tool.groups.delete([InternalId(id=g.id) for g in extra])
                    except ToolkitAPIError as e:
                        raise ResourceDeleteError(f"Unable to delete the extra groups.\n{e}")
                    print(f"  [bold green]OK[/] - Deleted {len(extra)} duplicated groups.")

        function_status = self.check_function_service_status(client, dry_run, has_added_capabilities)
        return VerifyAuthResult(cdf_toolkit_group.id, function_status)

    def _create_toolkit_group_in_cdf_interactive(
        self,
        client: ToolkitClient,
        toolkit_group: GroupRequest,
        all_groups: list[GroupResponse],
        is_interactive: bool,
        dry_run: bool,
    ) -> GroupResponse | None:
        if not is_interactive:
            raise AuthorizationError(
                f"Group {toolkit_group.name!r} does not exist in the CDF project. "
                "Please create the group and try again."
                f"\n{HINT_LEAD_TEXT}Run this command without --no-prompt to get assistance to create the group."
            )
        if not questionary.confirm(
            "Do you want to create it?",
            default=True,
        ).unsafe_ask():
            return None

        if dry_run:
            print(
                f"Would have created group {toolkit_group.name!r} with {len(toolkit_group.capabilities or [])} capabilities."
            )
            return None

        source_id = questionary.text(
            "What is the source id for the new group (typically a group id in the identity provider)?",
            validate=lambda value: value.strip() != "",
        ).unsafe_ask()

        toolkit_group.source_id = source_id
        if already_used := [group.name for group in all_groups if group.source_id == source_id]:
            self.warn(
                HighSeverityWarning(
                    f"The source id {source_id!r} is already used by the groups: {humanize_collection(already_used)!r}."
                )
            )
            if not questionary.confirm("This is NOT recommended. Do you want to continue?", default=False).unsafe_ask():
                return None

        return self._create_toolkit_group_in_cdf(client, toolkit_group)

    @staticmethod
    def _create_toolkit_group_in_cdf(
        client: ToolkitClient,
        toolkit_group: GroupRequest,
    ) -> GroupResponse:
        created = client.tool.groups.create([toolkit_group])[0]
        print(
            f"  [bold green]OK[/] - Created new group {created.name}. It now has {len(created.capabilities or [])} capabilities."
        )
        return created

    def _check_missing_capabilities(
        self,
        existing_group: GroupResponse,
        toolkit_group: GroupRequest,
        resource_name_by_acl_type: dict[type[AclType], list[str]],
        project: str,
        is_interactive: bool,
    ) -> Sequence[AclType]:
        print(f"\nChecking if the {existing_group.name} has the all required capabilities...")
        missing_capabilities = FlatCapabilities.from_group(existing_group, project).verify(
            [cap.acl for cap in toolkit_group.capabilities or []]
        )
        if not missing_capabilities:
            print(f"  [bold green]OK[/] - The {existing_group.name} has all the required capabilities.")
            return []

        for s in sorted(map(str, missing_capabilities)):
            self.warn(MissingCapabilityWarning(s))

        resource_names: set[str] = set()
        for acl in missing_capabilities:
            resource_names.update(resource_name_by_acl_type.get(type(acl), []))
        if resource_names:
            print("[bold yellow]INFO:[/] The missing capabilities are required for the following resources:")
            for resource_name in resource_names:
                print(f"    - {resource_name}")

        if not is_interactive:
            raise AuthorizationError(
                "The service principal/application does not have the required capabilities for the Toolkit to support all resources"
            )
        return missing_capabilities

    def _update_missing_capabilities(
        self,
        client: ToolkitClient,
        existing_group: GroupResponse,
        missing_capabilities: Sequence[AclType],
        dry_run: bool,
        project: str,
        data_modeling_status: Literal["HYBRID", "DATA_MODELING_ONLY"],
    ) -> bool:
        """Updates the missing capabilities. This assumes interactive mode."""
        updated_group = existing_group.as_request_resource()
        missing_group_caps = [GroupCapability(acl=acl) for acl in missing_capabilities]
        if updated_group.capabilities is None:
            updated_group.capabilities = list(missing_group_caps)
        else:
            updated_group.capabilities.extend(missing_group_caps)

        if data_modeling_status == "DATA_MODELING_ONLY":
            filtered_capabilities: list[GroupCapability] = []
            removed: list[str] = []
            for cap in updated_group.capabilities:
                if isinstance(cap.acl, AssetsAcl | RelationshipsAcl):
                    removed.append(str(cap))
                else:
                    filtered_capabilities.append(cap)
            if removed:
                self.console(
                    f"Removing {humanize_collection(removed)} as the project is in DATA_MODELING_ONLY mode."
                    f"These capabilities are not allowed in DATA_MODELING_ONLY projects.",
                    prefix="  [bold yellow]INFO[/] - ",
                )
            updated_group.capabilities = filtered_capabilities

        adding = FlatCapabilities.from_group(existing_group, project).verify(
            [cap.acl for cap in updated_group.capabilities]
        )
        capability_str = "capabilities" if len(adding) > 1 else "capability"
        if dry_run:
            print(f"Would have updated group {updated_group.name} with {len(adding)} new {capability_str}.")
            return False

        try:
            created = client.tool.groups.create([updated_group])[0]
        except ToolkitAPIError as e:
            raise ResourceCreationError(f"Unable to create group {updated_group.name}.\n{e}")
        try:
            client.tool.groups.delete([InternalId(id=existing_group.id)])
        except ToolkitAPIError as e:
            raise ResourceDeleteError(
                f"Failed to cleanup old version of the {existing_group.name}.\n{e}\n"
                f"It is recommended that you manually delete the Group with ID {existing_group.id},"
                f"such that you don't have a duplicated group in your CDF project."
            )
        print(f"  [bold green]OK[/] - Updated the group {created.name} with {len(adding)} new {capability_str}.")
        return True

    @staticmethod
    def _create_toolkit_group(required_capabilities: list[AclType], demo_user: str | None) -> GroupRequest:
        toolkit_group = GroupRequest(
            name=TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME if demo_user is None else TOOLKIT_DEMO_GROUP_NAME,
            capabilities=[GroupCapability(acl=acl) for acl in required_capabilities],
        )
        if demo_user:
            toolkit_group.members = [demo_user]
        return toolkit_group

    @staticmethod
    def _get_required_acls(
        client: ToolkitClient, data_modeling_status: Literal["HYBRID", "DATA_MODELING_ONLY"]
    ) -> tuple[list[AclType], dict[type[AclType], list[str]]]:
        required_acls: list[AclType] = []
        io_name_by_acl_type: dict[type[AclType], list[str]] = defaultdict(list)
        for crud_cls in cruds.RESOURCE_CRUD_LIST:
            if data_modeling_status == "DATA_MODELING_ONLY" and issubclass(crud_cls, AssetCRUD | RelationshipCRUD):
                # Assets and relationships are not supported on DATA_MODELING_ONLY projects.
                continue

            crud = crud_cls.create_loader(client)
            if crud.prerequisite_warning() is not None:
                continue
            for acl in crud.create_acl({"READ", "WRITE"}, AllScope()):
                required_acls.append(acl)
                io_name_by_acl_type[type(acl)].append(crud.display_name)

        required_acls = list(FlatCapabilities.merge_acls(required_acls))
        return required_acls, io_name_by_acl_type

    def check_has_any_access(self, client: ToolkitClient) -> InspectResponse:
        print("Checking basic project configuration...")
        try:
            inspect_response = client.tool.token.inspect()
            if inspect_response is None or len(inspect_response.capabilities) == 0:
                raise AuthorizationError(
                    "Valid authentication token, but it does not give any access rights."
                    " Check credentials (IDP_CLIENT_ID/IDP_CLIENT_SECRET or CDF_TOKEN)."
                )
            print("  [bold green]OK[/]")
        except ToolkitAPIError as e:
            raise AuthorizationError(
                "Not a valid authentication token. Check credentials (IDP_CLIENT_ID/IDP_CLIENT_SECRET or CDF_TOKEN)."
                "This could also be due to the service principal/application not having access to any Groups."
                f"\n{e}"
            )
        return inspect_response

    def check_has_project_access(self, inspect_response: InspectResponse, cdf_project: str) -> None:
        print("Checking projects that the service principal/application has access to...")
        if len(inspect_response.projects) == 0:
            raise AuthorizationError(
                "The service principal/application configured for this client does not have access to any projects."
            )
        print("\n".join(f"  - {p.project_url_name}" for p in inspect_response.projects))
        if cdf_project not in {p.project_url_name for p in inspect_response.projects}:
            raise AuthorizationError(
                f"The service principal/application configured for this client does not have access to the CDF_PROJECT={cdf_project!r}."
            )

    def check_has_group_access(self, client: ToolkitClient) -> None:
        print(
            "Checking basic project and group manipulation access rights "
            "(projectsAcl: LIST, READ and groupsAcl: LIST, READ, CREATE, UPDATE, DELETE)..."
        )
        missing_capabilities = client.tool.token.verify_acls(
            [
                ProjectsAcl(actions=["LIST", "READ"], scope=AllScope()),
                GroupsAcl(actions=["READ", "LIST", "CREATE", "UPDATE", "DELETE"], scope=AllScope()),
            ]
        )
        if not missing_capabilities:
            print("  [bold green]OK[/]")
            return
        self.warn(
            HighSeverityWarning(
                "The service principal/application configured for this client "
                "does not have the basic group write access rights."
            )
        )
        print("Checking basic group read access rights (projectsAcl: LIST, READ and groupsAcl: LIST, READ)...")
        missing = client.tool.token.verify_acls(
            [
                ProjectsAcl(actions=["LIST", "READ"], scope=AllScope()),
                GroupsAcl(actions=["READ", "LIST"], scope=AllScope()),
            ]
        )
        if not missing:
            print("  [bold green]OK[/] - can continue with checks.")
            return
        raise AuthorizationError(
            "Unable to continue, the service principal/application configured for this client does not"
            " have the basic read group access rights."
        )

    def check_identity_provider(self, client: ToolkitClient, cdf_project: str) -> None:
        print("Checking identity provider settings...")
        org = client.project.organization()
        oidc = org.oidc_configuration
        if oidc is None or oidc.token_url is None:
            self.warn(MediumSeverityWarning("No OIDC configuration or token URL found for the project"))
            return
        token_url = urllib.parse.urlparse(oidc.token_url)

        if token_url.hostname and token_url.hostname.endswith("login.windows.net"):
            # Typical Entra ID token URLs look like:
            #   https://login.windows.net/{tenant_id}/oauth2/token
            # We derive tenant_id from the path segments if possible
            path_parts = [p for p in token_url.path.split("/") if p]
            tenant_id = path_parts[0] if path_parts else "unknown"
            print(f"  [bold green]OK[/]: Microsoft Entra I with tenant id ({tenant_id}).")
        elif token_url.hostname and token_url.hostname.endswith("auth0.com"):
            tenant_id = token_url.hostname.split(".")[0]
            print(f"  [bold green]OK[/] - Auth0 with tenant id ({tenant_id}).")
        else:
            self.warn(MediumSeverityWarning(f"Unknown identity provider {token_url}"))
        access_claims = [c.claim_name for c in oidc.access_claims]
        print(
            f"  Matching on CDF group sourceIds will be done on any of these claims from the identity provider: {humanize_collection(access_claims)}"
        )

    def check_count_group_memberships(self, user_group: list[GroupResponse]) -> None:
        print("Checking CDF group memberships for the current client configured...")

        table = Table(title="CDF Group ids, Names, and Source Ids")
        table.add_column("Id", justify="left")
        table.add_column("Name", justify="left")
        table.add_column("Source Id", justify="left")
        for group in user_group:
            name = group.name
            if group.name == TOOLKIT_SERVICE_PRINCIPAL_GROUP_NAME:
                name = f"[bold]{group.name}[/]"
            table.add_row(str(group.id), name, group.source_id)
        print(table)

        if len(user_group) > 1:
            self.warn(
                LowSeverityWarning(
                    "This service principal/application gets its access rights from more than one CDF group."
                    "\nThis is not recommended. The group matching the group config file is marked in "
                    "bold above if it is present."
                )
            )
        else:
            print("  [bold green]OK[/] - Only one group is used for this service principal/application.")

    def check_source_id_usage(self, all_groups: list[GroupResponse], cdf_toolkit_group: GroupResponse) -> None:
        reuse_source_id = [
            group.name
            for group in all_groups
            if group.source_id == cdf_toolkit_group.source_id and group.id != cdf_toolkit_group.id
        ]
        if reuse_source_id:
            group_names_str = humanize_collection(reuse_source_id)
            self.warn(
                MediumSeverityWarning(
                    f"The following groups have the same source id, {cdf_toolkit_group.source_id},\n"
                    f"as the {cdf_toolkit_group.name!r} group: \n    {group_names_str!r}.\n"
                    f"It is recommended that only the {cdf_toolkit_group.name!r} group has this source id."
                )
            )

    def check_duplicated_names(
        self, all_groups: list[GroupResponse], cdf_toolkit_group: GroupResponse
    ) -> list[GroupResponse]:
        extra = [
            group for group in all_groups if group.name == cdf_toolkit_group.name and group.id != cdf_toolkit_group.id
        ]
        if extra:
            self.warn(
                MediumSeverityWarning(
                    f"There are multiple groups with the same name {cdf_toolkit_group.name} in the CDF project."
                    "           It is recommended that this admin (CI/CD) application/service principal "
                    "only is member of one group in the identity provider. Suggest you delete all but one"
                    "           of the groups with the same name."
                )
            )

        return extra

    def check_function_service_status(
        self, client: ToolkitClient, dry_run: bool, has_added_capabilities: bool
    ) -> str | None:
        print("Checking function service status...")
        has_function_read_access = self.has_function_rights(client, ["READ"], has_added_capabilities)
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
            has_function_write_access = self.has_function_rights(client, ["WRITE"], has_added_capabilities)
            if not has_function_write_access:
                self.warn(HighSeverityWarning("Cannot activate function service, missing function write access."))
                return function_status.status

            if client.config.is_private_link:
                print(
                    "  [bold yellow]INFO:[/] Function service has not been activated. "
                    "Function activation must be done manually."
                )
                return function_status.status
            try:
                client.functions.activate()
            except CogniteAPIError as e:
                self.warn(HighSeverityWarning(f"Unable to activate function service.\n{e}"))
                return function_status.status
            print(
                "  [bold green]OK[/] - Function service has been activated. This may take up to 2 hours to take effect."
            )
        else:
            print("  [bold green]OK[/] - Function service has been activated.")

        return function_status.status

    def has_function_rights(
        self, client: ToolkitClient, actions: list[Literal["READ", "WRITE"]], has_added_capabilities: bool
    ) -> bool:
        t0 = time.perf_counter()
        while not (
            has_function_access := not client.tool.token.verify_acls([FunctionsAcl(actions=actions, scope=AllScope())])
        ):
            if has_added_capabilities and (time.perf_counter() - t0 < 5.0):
                sleep(1.0)
            else:
                break
        return has_function_access
