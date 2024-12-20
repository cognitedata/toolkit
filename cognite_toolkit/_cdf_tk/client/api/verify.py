import warnings
from collections.abc import Sequence
from typing import TYPE_CHECKING

from cognite.client.config import ClientConfig
from cognite.client.data_classes.capabilities import Capability
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client.api_client import ToolkitAPI
from cognite_toolkit._cdf_tk.constants import URL
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient


class VerifyAPI(ToolkitAPI):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: "ToolkitClient") -> None:
        super().__init__(config, api_version, cognite_client)
        self._token_inspect: TokenInspection | None = None

    @property
    def token_inspect(self) -> TokenInspection:
        if self._token_inspect is None:
            with warnings.catch_warnings():
                # If the user has unknown capabilities, we don't want the user to see the warning:
                # "UserWarning: Unknown capability '<unknown warning>'.
                warnings.simplefilter("ignore")
                try:
                    self._token_inspect = self._toolkit_client.iam.token.inspect()
                except CogniteAPIError as e:
                    raise AuthorizationError(
                        f"Don't seem to have any access rights. {e}\n"
                        f"Please visit [link={URL.configure_access}]the documentation[/link] "
                        f"and ensure you have configured your access correctly."
                    ) from e
        return self._token_inspect

    def authorization(self, capabilities: Capability | Sequence[Capability]) -> list[Capability]:
        """Verify that the client has correct credentials and required access rights

        Args:
            capabilities (Capability | Sequence[Capability]): access capabilities to verify

        Returns:
            list[Capability]: list of missing capabilities

        """
        with warnings.catch_warnings():
            # If the user has unknown capabilities, we don't want the user to see the warning:
            # "UserWarning: Unknown capability '<unknown warning>' will be ignored in comparison"
            # This is irrelevant for the user as we are only checking the capabilities that are known.
            warnings.simplefilter("ignore")
            return self._toolkit_client.iam.compare_capabilities(self.token_inspect.capabilities, capabilities)

    @staticmethod
    def create_error(missing_capabilities: Sequence[Capability], action: str | None = None) -> AuthorizationError:
        """Create an AuthorizationError with a message that lists the missing capabilities

        Args:
            missing_capabilities (Sequence[Capability]): capabilities that are missing
            action (str, optional): action that requires the capabilities. Defaults to None.

        """
        if not missing_capabilities:
            raise ValueError("Bug in Toolkit. Tried creating an AuthorizationError without any missing capabilities.")
        missing = "  - \n".join(repr(c) for c in missing_capabilities)
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
