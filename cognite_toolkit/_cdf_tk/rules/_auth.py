from collections.abc import Iterable
from dataclasses import replace
from functools import cached_property
from pathlib import Path
from typing import Any, NamedTuple

from cognite.client.exceptions import CogniteAuthError
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.commands.auth.data_classes._constants import CLIENT_NAME
from cognite_toolkit._cdf_tk.commands.auth.data_classes._environment_variables import EnvironmentVariables
from cognite_toolkit._cdf_tk.constants import ENV_VAR_PATTERN
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.exceptions import AuthenticationError, ToolkitKeyError, ToolkitMissingValueError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    FunctionScheduleIO,
    HostedExtractorDestinationIO,
    ResourceType,
    TransformationIO,
    WorkflowTriggerIO,
)
from cognite_toolkit._cdf_tk.rules._base import (
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
    quote_identifier,
    with_position,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.yaml_classes.authentication import AuthenticationClientIdSecret, OIDCCredential


class _CredentialsLocation(NamedTuple):
    key: str
    display_name: str


_CREDENTIALS_LOCATION_BY_TYPE: dict[ResourceType, _CredentialsLocation] = {
    WorkflowTriggerIO.as_resource_type(): _CredentialsLocation("authentication", "workflow trigger"),
    TransformationIO.as_resource_type(): _CredentialsLocation("authentication", "transformation"),
    HostedExtractorDestinationIO.as_resource_type(): _CredentialsLocation(
        "credentials", "hosted extractor destination"
    ),
    FunctionScheduleIO.as_resource_type(): _CredentialsLocation("authentication", "function schedule"),
}

_OIDC_KEYS = frozenset({"tokenUri", "cdfProjectName", "scopes", "audience"})


def _read_credentials(raw: Any) -> Iterable[tuple[str, AuthenticationClientIdSecret]]:
    """The credentials in the YAML, together with the operation they are used for, e.g., 'read' for transformations.

    Transformations can have separate credentials for reading and writing, or one set used for both.
    """
    if not isinstance(raw, dict):
        return
    if "read" in raw or "write" in raw:
        candidates = [(operation, raw[operation]) for operation in ("read", "write") if operation in raw]
    else:
        candidates = [("", raw)]
    for operation, value in candidates:
        if not isinstance(value, dict):
            continue
        credential_cls = OIDCCredential if _OIDC_KEYS.intersection(value) else AuthenticationClientIdSecret
        try:
            yield operation, credential_cls.model_validate(value)
        except ValidationError:
            # Malformed credentials are reported by the syntax validation.
            continue


class CredentialsCheck(ToolkitRuleSet):
    """Validates that the credentials of resources that run on behalf of a service principal can authenticate.

    The credentials are exchanged for a token with the identity provider configured in the environment, and
    the token is verified against CDF.
    """

    RESOURCE_TYPES = frozenset(_CREDENTIALS_LOCATION_BY_TYPE)
    CONTEXT = frozenset({"local"})
    DISPLAY_NAME = "Credentials"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(code="skip", message="Credentials validation requires a client to authenticate.")
        if isinstance(self._environment, Exception):
            return RuleSetStatus(
                code="skip",
                message=f"Credentials validation requires the identity provider settings: {self._environment}",
            )
        return RuleSetStatus(
            code="ready",
            message=(
                "Will validate that the credentials of workflow triggers, transformations, hosted extractor "
                "destinations, and function schedules can authenticate."
            ),
        )

    @cached_property
    def _environment(self) -> EnvironmentVariables | Exception:
        try:
            return EnvironmentVariables.create_from_environment()
        except (AuthenticationError, ToolkitMissingValueError) as error:
            return error

    @cached_property
    def _failure_by_credentials(self) -> dict[tuple[tuple[str, str], ...], str | None]:
        """Resources often share credentials, so each set of credentials is only authenticated once."""
        return {}

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        environment = self._environment
        if self.client is None or isinstance(environment, Exception):
            return
        environment_variables = environment.dump()
        for resource in self.resources:
            location = _CREDENTIALS_LOCATION_BY_TYPE.get(resource.type)
            if location is None or not resource.has_valid_syntax:
                continue
            try:
                yield from self._validate_resource(resource, location, environment, environment_variables)
            except Exception as error:
                yield InternalValidatorException(
                    message=(
                        f"Failed to validate the credentials of {location.display_name} "
                        f"{quote_identifier(resource.identifier)}: {error}"
                    ),
                    source=str(resource.identifier),
                )

    def _validate_resource(
        self,
        resource: ResourceRef,
        location: _CredentialsLocation,
        environment: EnvironmentVariables,
        environment_variables: dict[str, str | None],
    ) -> Iterable[Insight]:
        source_file = resource.source
        if not isinstance(source_file, Path):
            return
        raw_credentials = resource.as_yaml(environment_variables).get(location.key)
        name = f"{location.display_name} {quote_identifier(resource.identifier)}"
        for operation, credential in _read_credentials(raw_credentials):
            prefix = f"{operation} " if operation else ""
            secret = credential.client_secret.get_secret_value()
            if missing := sorted({*ENV_VAR_PATTERN.findall(credential.client_id), *ENV_VAR_PATTERN.findall(secret)}):
                yield BuildWarning(
                    code="UNVERIFIED-CREDENTIALS",
                    message=(
                        f"Could not verify the {prefix}credentials of the {name}. The environment variables "
                        f"{humanize_collection(missing)} are not set."
                    ),
                    fix="Set the environment variables used by the credentials before validating.",
                    source_file=source_file,
                    alpha=Flags.ALPHA_RULES.is_enabled(),
                )
                continue
            if failure := self._authenticate(credential, environment):
                yield with_position(
                    BuildError(
                        code="INVALID-CREDENTIALS",
                        message=f"The {prefix}credentials of the {name} failed to authenticate. {failure}",
                        fix=(
                            "Ensure that the clientId and clientSecret are correct and that the client secret "
                            "has not expired."
                        ),
                        source_file=source_file,
                        alpha=Flags.ALPHA_RULES.is_enabled(),
                    ),
                    keys=[location.key],
                    variables=resource.variables,
                )

    def _authenticate(self, credential: AuthenticationClientIdSecret, environment: EnvironmentVariables) -> str | None:
        """Returns the reason the credentials failed to authenticate, or None if they are valid."""
        changes: dict[str, str] = {
            "LOGIN_FLOW": "client_credentials",
            "IDP_CLIENT_ID": credential.client_id,
            "IDP_CLIENT_SECRET": credential.client_secret.get_secret_value(),
        }
        if isinstance(credential, OIDCCredential):
            changes["IDP_TOKEN_URL"] = credential.token_uri
            changes["CDF_PROJECT"] = credential.cdf_project_name
            if credential.audience:
                changes["IDP_AUDIENCE"] = credential.audience
            if credential.scopes:
                scopes = credential.scopes
                changes["IDP_SCOPES"] = ",".join(scopes) if isinstance(scopes, list) else scopes
        cache_key = tuple(sorted(changes.items()))
        if cache_key not in self._failure_by_credentials:
            self._failure_by_credentials[cache_key] = self._create_and_verify_token(replace(environment, **changes))
        return self._failure_by_credentials[cache_key]

    @staticmethod
    def _create_and_verify_token(environment: EnvironmentVariables) -> str | None:
        try:
            credentials = environment._get_oauth_client_credentials()
        except (ToolkitMissingValueError, ToolkitKeyError) as error:
            return str(error)
        try:
            credentials.authorization_header()
        except CogniteAuthError as error:
            return f"Failed to create a token: {error}"
        config = ToolkitClientConfig(
            client_name=CLIENT_NAME,
            project=environment.CDF_PROJECT,
            credentials=credentials,
            base_url=environment.cdf_url,
            timeout=environment.CDF_CLIENT_TIMEOUT,
        )
        try:
            ToolkitClient(config).token.inspect()
        except ToolkitAPIError as error:
            return f"The token is not valid: {error}"
        return None
