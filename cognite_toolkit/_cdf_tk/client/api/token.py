from collections.abc import Sequence
from functools import cache, cached_property

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage
from cognite_toolkit._cdf_tk.client.resource_classes.group import Acl, AclType, Scope
from cognite_toolkit._cdf_tk.client.resource_classes.token import FlatCapabilities, InspectResponse
from cognite_toolkit._cdf_tk.constants import URL
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.utils import humanize_collection


class ToolkitTokenAPI:
    def __init__(self, http_client: HTTPClient):
        self._http_client = http_client

    @cache
    def inspect(self) -> InspectResponse:
        """Inspect the current token and return its capabilities and scopes."""
        config = self._http_client.config
        url = f"{config.base_url}/api/v1/token/inspect"
        request = RequestMessage(
            endpoint_url=url,
            method="GET",
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        result = InspectResponse.model_validate_json(response.body)
        result.project = self._http_client.config.project
        return result

    @cached_property
    def project_capabilities(self) -> FlatCapabilities:
        token: InspectResponse = self.inspect()
        return token.to_project_capabilities()

    def verify_acls(self, required_acls: Sequence[AclType]) -> Sequence[AclType]:
        """Verify that the current token has the required ACLs, for the current project. Returns the list of missing ACLs."""
        try:
            capabilities = self.project_capabilities
        except AuthorizationError as e:
            raise AuthorizationError(
                f"Failed to validate {humanize_collection([repr(acl) for acl in required_acls])}. \n{e!s}"
            )
        return capabilities.verify(required_acls)

    def check_available_scopes(self, acl_cls: type[Acl], actions: Sequence[str]) -> list[Scope]:
        """Check the available scopes for the given ACL class and actions. Returns a list of scopes that are available for all actions."""
        return self.project_capabilities.get_available_scopes(acl_cls, actions)

    def create_error(self, missing_capabilities: Sequence[Acl], action: str | None = None) -> AuthorizationError:
        """Create an AuthorizationError with a message that lists the missing capabilities

        Args:
            missing_capabilities (Sequence[Acl]): capabilities that are missing
            action (str, optional): action that requires the capabilities. Defaults to None.

        """
        if not missing_capabilities:
            raise ValueError("Bug in Toolkit. Tried creating an AuthorizationError without any missing capabilities.")
        missing = "\n".join(f"  - {c!r}" for c in missing_capabilities)
        first_sentence = "Don't have correct access rights"
        if action:
            first_sentence += f" to {action}."
        else:
            first_sentence += "."

        return AuthorizationError(
            f"{first_sentence} Missing:\n{missing}\n"
            f"Please [blue][link={URL.auth_toolkit}]click here[/link][/blue] to visit the documentation "
            "and ensure that you have setup authentication for the CDF toolkit correctly."
        )
