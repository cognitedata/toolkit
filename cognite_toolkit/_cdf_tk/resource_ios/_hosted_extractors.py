from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, final

from typing_extensions import TypeForm

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.group import (
    AclType,
    AllScope,
    HostedExtractorsAcl,
    ScopeDefinition,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationRequest,
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    HostedExtractorJobRequest,
    HostedExtractorJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_mapping import (
    HostedExtractorMappingRequest,
    HostedExtractorMappingResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    BasicAuthenticationRequest,
    ClientCredentialAuthenticationRequest,
    EventHubSourceRequest,
    EventHubSourceResponse,
    HostedExtractorSourceRequest,
    HostedExtractorSourceRequestUnion,
    HostedExtractorSourceResponseUnion,
    HTTPBasicAuthenticationRequest,
    KafkaSourceRequest,
    MQTTBrokerSourceRequest,
    MQTTSourceRequest,
    RESTSourceRequest,
    ScramShaAuthenticationRequest,
    UnknownAuthenticationRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.session import ClientCredentialsSessionRequest
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported
from cognite_toolkit._cdf_tk.resource_ios._base_ios import ResourceIO
from cognite_toolkit._cdf_tk.resource_ios._data_organization import DataSetsIO
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils import calculate_secure_hash, humanize_collection
from cognite_toolkit._cdf_tk.utils.cdf import read_auth
from cognite_toolkit._cdf_tk.utils.file import sanitize_filename
from cognite_toolkit._cdf_tk.yaml_classes import (
    HostedExtractorDestinationYAML,
    HostedExtractorJobYAML,
    HostedExtractorMappingYAML,
    HostedExtractorSourceYAML,
)


@final
class HostedExtractorSourceIO(
    ResourceIO[
        ExternalId,
        HostedExtractorSourceRequestUnion,
        HostedExtractorSourceResponseUnion,
        HostedExtractorSourceYAML,
    ]
):
    folder_name = "hosted_extractors"
    # These are Annotated unions, not classes. Call sites load them through the TypeAdapter.
    resource_cls: TypeForm[HostedExtractorSourceResponseUnion] = HostedExtractorSourceResponseUnion
    resource_write_cls: TypeForm[HostedExtractorSourceRequestUnion] = HostedExtractorSourceRequestUnion
    kind = "Source"
    yaml_cls = HostedExtractorSourceYAML
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Sources/operation/create_sources"
    _SupportedSources = (
        MQTTSourceRequest,
        KafkaSourceRequest,
        RESTSourceRequest,
        EventHubSourceRequest,
        MQTTBrokerSourceRequest,
    )

    class _MetadataKey:
        _prefix = "toolkit-hash"
        AUTH_BASIC_PASSWORD = f"{_prefix}-auth-basic-password"
        AUTH_CLIENT_SECRET = f"{_prefix}-auth-client-secret"
        AUTH_VALUE = f"{_prefix}-auth-value"
        CA_CERTIFICATE = f"{_prefix}-ca-certificate"
        AUTH_CERTIFICATE = f"{_prefix}-auth-certificate"

        @classmethod
        def auth_unknown(cls, field: str) -> str:
            return f"{cls._prefix}-auth-{field}"

    # The certificate fields are objects that may contain sensitive strings (private key and key password).
    # The API only returns certificate metadata (thumbprint/expiry), so we hash the local value into the
    # resource metadata to detect changes.
    _CERTIFICATE_KEYS: tuple[tuple[str, str], ...] = (
        ("caCertificate", _MetadataKey.CA_CERTIFICATE),
        ("authCertificate", _MetadataKey.AUTH_CERTIFICATE),
    )

    @property
    def display_name(self) -> str:
        return "hosted extractor sources"

    @classmethod
    def get_id(
        cls, item: HostedExtractorSourceRequestUnion | HostedExtractorSourceResponseUnion | ExternalId | dict
    ) -> ExternalId:
        if isinstance(item, ExternalId):
            return item
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_filename(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[HostedExtractorSourceRequestUnion]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield HostedExtractorsAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_dependencies(cls, resource: HostedExtractorSourceYAML) -> "Iterable[tuple[type[ResourceIO], Identifier]]":
        return []

    def create(self, items: Sequence[HostedExtractorSourceRequestUnion]) -> list[HostedExtractorSourceResponseUnion]:
        to_create: list[HostedExtractorSourceRequestUnion] = []
        to_skip: list[MQTTBrokerSourceRequest] = []
        for item in items:
            if isinstance(item, MQTTBrokerSourceRequest):
                to_skip.append(item)
            else:
                to_create.append(item)
        if to_skip:
            HighSeverityWarning(
                f"{len(to_skip)} MQTT broker sources will not be created by Toolkit. You will need to create them manually through the CDF API, Toolkit can update them once they are created. "
                f"The reason is that MQTT broker responses return auth information in the response. If you had created them through Toolkit this would be lost."
                f"The following MQTT broker sources will not be created: {humanize_collection(to_skip)}"
            ).print_warning(console=self.client.console)

        return self.client.hosted_extractors.sources.create(to_create)

    def retrieve(self, ids: Sequence[ExternalId]) -> list[HostedExtractorSourceResponseUnion]:
        return self.client.hosted_extractors.sources.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[HostedExtractorSourceRequestUnion]) -> list[HostedExtractorSourceResponseUnion]:
        return self.client.hosted_extractors.sources.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> list[ExternalId]:
        id_list = list(ids)
        if not id_list:
            return []
        self.client.hosted_extractors.sources.delete(id_list, ignore_unknown_ids=True)
        return id_list

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[HostedExtractorSourceResponseUnion]:
        for sources in self.client.hosted_extractors.sources.iterate(limit=None):
            yield from sources

    def dump_resource(
        self, resource: HostedExtractorSourceResponseUnion, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.dump()
        dumped.pop("createdTime", None)
        dumped.pop("lastUpdatedTime", None)
        if isinstance(resource, EventHubSourceResponse):
            if not (local and "keyName" in local):
                # Deprecated, but still returned by the API even if it is not set.
                dumped.pop("keyName", None)

        local_auth = local.get("authentication") if local else None
        if isinstance(local_auth, dict) and isinstance(dumped.get("authentication"), dict):
            # The API does not return sensitive strings (passwords, secrets, tokens). We set them equal
            # to the local values as any actual change will be caught by the hash comparison and redeployed.
            # Furthermore, it should not confuse the user as all sensitive strings are hidden from terminal output.
            dumped_auth = dumped["authentication"]
            for field_name, _ in self._sensitive_auth_fields(local_auth):
                if field_name in local_auth:
                    dumped_auth[field_name] = local_auth[field_name]

        if dumped.get("scheme") == "https" and local and "scheme" not in local:
            # We remove the default set by the API to avoid unnecessary redeploys.
            dumped.pop("scheme")

        # API returns certificate metadata (thumbprint/expiry) that cannot be redeployed. Re-inject the
        # local certificate so an unchanged source is not flagged as changed. Any change to the certificate
        # (including its private key or key password) is caught by the hash stored in metadata.
        for cert_key, _ in self._CERTIFICATE_KEYS:
            if local and isinstance(local.get(cert_key), dict | str):
                dumped[cert_key] = local[cert_key]
            else:
                dumped.pop(cert_key, None)
        return dumped

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        raw_data_list = super().load_resource_file(filepath, environment_variables)
        for raw_data in raw_data_list:
            metadata = raw_data.get("metadata", {}) or {}
            authentication = raw_data.get("authentication")
            if isinstance(authentication, dict):
                for field_name, metadata_key in self._sensitive_auth_fields(authentication):
                    secret = authentication.get(field_name)
                    if isinstance(secret, str) and secret:
                        metadata[metadata_key] = calculate_secure_hash(secret, shorten=True)
            for cert_key, metadata_key in self._CERTIFICATE_KEYS:
                certificate = raw_data.get(cert_key)
                if isinstance(certificate, dict | str):
                    metadata[metadata_key] = calculate_secure_hash(certificate, shorten=True)
            if metadata:
                raw_data["metadata"] = metadata
        return raw_data_list

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> HostedExtractorSourceRequestUnion:
        loaded = HostedExtractorSourceRequest.validate_python(resource)
        if not isinstance(loaded, self._SupportedSources):
            # We need to explicitly check for the supported types as we need to ensure that we know
            # what the sensitive strings are for each type of source.
            # Technically, we could support any new source type, but
            # we would risk printing sensitive strings in the terminal.
            raise ToolkitNotSupported(
                f"Hosted extractor source type {type(loaded).__name__} is not supported. "
                f"Please contact support to request support for this source type."
            )
        return loaded

    def sensitive_strings(self, item: HostedExtractorSourceRequestUnion) -> Iterable[str]:
        if isinstance(item, MQTTSourceRequest | KafkaSourceRequest | RESTSourceRequest) and item.authentication:
            yield from self._sensitive_auth_strings(item.authentication)
        if (
            isinstance(item, MQTTSourceRequest | KafkaSourceRequest)
            and item.auth_certificate
            and item.auth_certificate.key_password
        ):
            yield item.auth_certificate.key_password

    @staticmethod
    def _sensitive_auth_strings(
        auth: BasicAuthenticationRequest
        | ClientCredentialAuthenticationRequest
        | ScramShaAuthenticationRequest
        | HTTPBasicAuthenticationRequest
        | UnknownAuthenticationRequest,
    ) -> Iterable[str]:
        if isinstance(auth, BasicAuthenticationRequest | ScramShaAuthenticationRequest) and auth.password:
            yield auth.password
        elif isinstance(auth, ClientCredentialAuthenticationRequest):
            yield auth.client_secret
        elif isinstance(auth, HTTPBasicAuthenticationRequest):
            yield auth.value
        elif isinstance(auth, UnknownAuthenticationRequest):
            # Assume all keys are sensitive. Ensuring that we do not expose secrets.
            yield from set(auth.dump().keys()) - {"type"}

    @classmethod
    def _sensitive_auth_fields(cls, authentication: dict[str, Any]) -> Iterable[tuple[str, str]]:
        """Yields (field name in the authentication object, metadata hash key) for each sensitive string.

        The hash of each sensitive string is stored in the resource metadata so that a change to a secret
        is detected (and triggers a redeploy) even though the API never returns the secret itself.
        """
        auth_type = authentication.get("type")
        if auth_type in ("basic", "scramSha256", "scramSha512"):
            yield "password", cls._MetadataKey.AUTH_BASIC_PASSWORD
        elif auth_type == "clientCredentials":
            yield "clientSecret", cls._MetadataKey.AUTH_CLIENT_SECRET
        elif auth_type in ("header", "query"):
            yield "value", cls._MetadataKey.AUTH_VALUE
        else:
            # Unknown authentication type. Assume every field (except the discriminator 'type') is
            # sensitive to ensure we do not expose secrets.
            for field in authentication:
                if field != "type":
                    yield field, cls._MetadataKey.auth_unknown(field)


@final
class HostedExtractorDestinationIO(
    ResourceIO[
        ExternalId,
        HostedExtractorDestinationRequest,
        HostedExtractorDestinationResponse,
        HostedExtractorDestinationYAML,
    ]
):
    folder_name = "hosted_extractors"
    resource_cls = HostedExtractorDestinationResponse
    resource_write_cls = HostedExtractorDestinationRequest
    dependencies = frozenset({DataSetsIO})
    kind = "Destination"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Destinations/operation/create_destinations"
    yaml_cls = HostedExtractorDestinationYAML

    def __init__(self, client: ToolkitClient):
        super().__init__(client)
        self._authentication_by_id: dict[str, ClientCredentialsSessionRequest] = {}

    @property
    def display_name(self) -> str:
        return "hosted extractor destinations"

    @classmethod
    def get_id(
        cls, item: HostedExtractorDestinationRequest | HostedExtractorDestinationResponse | ExternalId | dict
    ) -> ExternalId:
        if isinstance(item, ExternalId):
            return item
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_filename(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[HostedExtractorDestinationRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield HostedExtractorsAcl(actions=sorted(actions), scope=scope)

    def create(self, items: Sequence[HostedExtractorDestinationRequest]) -> list[HostedExtractorDestinationResponse]:
        return self.client.hosted_extractors.destinations.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[HostedExtractorDestinationResponse]:
        return self.client.hosted_extractors.destinations.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[HostedExtractorDestinationRequest]) -> list[HostedExtractorDestinationResponse]:
        return self.client.hosted_extractors.destinations.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> list[ExternalId]:
        id_list = list(ids)
        if not id_list:
            return []
        self.client.hosted_extractors.destinations.delete(id_list, ignore_unknown_ids=True)
        return id_list

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[HostedExtractorDestinationResponse]:
        for destinations in self.client.hosted_extractors.destinations.iterate(limit=None):
            yield from destinations

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> HostedExtractorDestinationRequest:
        if raw_auth := resource.pop("credentials", None):
            external_id = self.get_id(resource).external_id
            credentials = read_auth(
                raw_auth,
                self.client.config,
                external_id,
                "hosted extractor destination",
                console=self.console,
            )
            self._authentication_by_id[external_id] = credentials
            if is_dry_run:
                resource["credentials"] = {"nonce": "dummy_nonce"}
            else:
                session = self.client.sessions.create_single(credentials)
                resource["credentials"] = {"nonce": session.nonce}
        if ds_external_id := resource.pop("targetDataSetExternalId", None):
            resource["targetDataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return HostedExtractorDestinationRequest.model_validate(resource)

    def dump_resource(
        self, resource: HostedExtractorDestinationResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        # Credentials cannot be compared (API never returns them). Dumping only the identifier
        # when a local YAML exists makes deploy always upsert.
        if local:
            HighSeverityWarning(
                "Destinations that contain credentials are always considered as changed and will be redeployed every time"
            ).print_warning(console=self.client.console)
            return self.dump_id(self.get_id(resource))
        dumped = resource.as_request_resource().dump()
        if data_set_id := dumped.pop("targetDataSetId", None):
            dumped["targetDataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        return dumped

    @classmethod
    def get_dependencies(
        cls, resource: HostedExtractorDestinationYAML
    ) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.target_data_set_external_id:
            yield DataSetsIO, ExternalId(external_id=resource.target_data_set_external_id)

    def sensitive_strings(self, item: HostedExtractorDestinationRequest) -> Iterable[str]:
        if item.credentials:
            yield item.credentials.nonce
        id_ = self.get_id(item)
        if id_.external_id in self._authentication_by_id:
            yield self._authentication_by_id[id_.external_id].client_secret


@final
class HostedExtractorJobIO(
    ResourceIO[ExternalId, HostedExtractorJobRequest, HostedExtractorJobResponse, HostedExtractorJobYAML]
):
    folder_name = "hosted_extractors"
    resource_cls = HostedExtractorJobResponse
    resource_write_cls = HostedExtractorJobRequest
    dependencies = frozenset({HostedExtractorSourceIO, HostedExtractorDestinationIO})
    kind = "Job"
    yaml_cls = HostedExtractorJobYAML
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Jobs/operation/create_jobs"

    @property
    def display_name(self) -> str:
        return "hosted extractor jobs"

    @classmethod
    def get_id(cls, item: HostedExtractorJobRequest | HostedExtractorJobResponse | ExternalId | dict) -> ExternalId:
        if isinstance(item, ExternalId):
            return item
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_filename(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[HostedExtractorJobRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield HostedExtractorsAcl(actions=sorted(actions), scope=scope)

    def dump_resource(
        self, resource: HostedExtractorJobResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if not dumped.get("config") and "config" not in local:
            dumped.pop("config", None)
        cdf_format: dict | None = dumped["format"] if isinstance(dumped.get("format"), dict) else None
        if cdf_format:
            if isinstance(local_format := local.get("format"), dict):
                if cdf_format.get("encoding") == "utf8" and "encoding" not in local_format:
                    # The API returns a default encoding of "utf8" even if it is not set. We remove it to avoid unnecessary redeploys.
                    cdf_format.pop("encoding")
            if cdf_format.get("type") == "custom" and "expression" in cdf_format:
                # The API returns a default expression fetched from the mapping itself.
                # removing it.
                cdf_format.pop("expression")

        return dumped

    def create(self, items: Sequence[HostedExtractorJobRequest]) -> list[HostedExtractorJobResponse]:
        return self.client.hosted_extractors.jobs.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[HostedExtractorJobResponse]:
        return self.client.hosted_extractors.jobs.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[HostedExtractorJobRequest]) -> list[HostedExtractorJobResponse]:
        return self.client.hosted_extractors.jobs.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> list[ExternalId]:
        id_list = list(ids)
        if not id_list:
            return []
        self.client.hosted_extractors.jobs.delete(id_list, ignore_unknown_ids=True)
        return id_list

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[HostedExtractorJobResponse]:
        for jobs in self.client.hosted_extractors.jobs.iterate(limit=None):
            yield from jobs

    @classmethod
    def get_dependencies(cls, resource: HostedExtractorJobYAML) -> Iterable[tuple[type[ResourceIO], Identifier]]:
        if resource.source_id:
            yield HostedExtractorSourceIO, ExternalId(external_id=resource.source_id)
        if resource.destination_id:
            yield HostedExtractorDestinationIO, ExternalId(external_id=resource.destination_id)


@final
class HostedExtractorMappingIO(
    ResourceIO[
        ExternalId,
        HostedExtractorMappingRequest,
        HostedExtractorMappingResponse,
        HostedExtractorMappingYAML,
    ]
):
    folder_name = "hosted_extractors"
    resource_cls = HostedExtractorMappingResponse
    resource_write_cls = HostedExtractorMappingRequest
    # This is not an explicit dependency, however, adding it here as mapping will should be deployed after source.
    dependencies = frozenset({HostedExtractorSourceIO})
    kind = "Mapping"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Mappings/operation/create_mappings"
    yaml_cls = HostedExtractorMappingYAML

    @property
    def display_name(self) -> str:
        return "hosted extractor mappings"

    @classmethod
    def get_id(
        cls, item: HostedExtractorMappingRequest | HostedExtractorMappingResponse | ExternalId | dict
    ) -> ExternalId:
        if isinstance(item, ExternalId):
            return item
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_filename(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_minimum_scope(cls, items: Sequence[HostedExtractorMappingRequest]) -> ScopeDefinition:
        return AllScope()

    @classmethod
    def create_acl(cls, actions: set[Literal["READ", "WRITE"]], scope: ScopeDefinition) -> Iterable[AclType]:
        if isinstance(scope, AllScope):
            yield HostedExtractorsAcl(actions=sorted(actions), scope=scope)

    @classmethod
    def get_dependencies(cls, resource: HostedExtractorMappingYAML) -> "Iterable[tuple[type[ResourceIO], Identifier]]":
        return []

    def create(self, items: Sequence[HostedExtractorMappingRequest]) -> list[HostedExtractorMappingResponse]:
        return self.client.hosted_extractors.mappings.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[HostedExtractorMappingResponse]:
        return self.client.hosted_extractors.mappings.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[HostedExtractorMappingRequest]) -> list[HostedExtractorMappingResponse]:
        return self.client.hosted_extractors.mappings.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> list[ExternalId]:
        id_list = list(ids)
        if not id_list:
            return []
        self.client.hosted_extractors.mappings.delete(id_list, ignore_unknown_ids=True)
        return id_list

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[HostedExtractorMappingResponse]:
        for mappings in self.client.hosted_extractors.mappings.iterate(limit=None):
            yield from mappings
