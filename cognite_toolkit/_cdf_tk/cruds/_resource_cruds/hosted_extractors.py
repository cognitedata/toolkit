from collections.abc import Hashable, Iterable, Sequence
from pathlib import Path
from typing import Any

from cognite.client.data_classes import ClientCredentials
from cognite.client.data_classes.capabilities import Capability, HostedExtractorsAcl
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers.identifiers import ExternalId
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
    HostedExtractorSourceRequest,
    HostedExtractorSourceRequestUnion,
    HostedExtractorSourceResponseUnion,
    HTTPBasicAuthenticationRequest,
    KafkaSourceRequest,
    MQTTSourceRequest,
    RESTSourceRequest,
    ScramShaAuthenticationRequest,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotSupported
from cognite_toolkit._cdf_tk.resource_classes import (
    HostedExtractorDestinationYAML,
    HostedExtractorJobYAML,
    HostedExtractorMappingYAML,
    HostedExtractorSourceYAML,
)
from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning

from .data_organization import DataSetsCRUD


class HostedExtractorSourceCRUD(
    ResourceCRUD[ExternalId, HostedExtractorSourceRequestUnion, HostedExtractorSourceResponseUnion]
):
    folder_name = "hosted_extractors"
    resource_cls = HostedExtractorSourceResponseUnion  # type: ignore[assignment]
    resource_write_cls = HostedExtractorSourceRequestUnion  # type: ignore[assignment]
    kind = "Source"
    yaml_cls = HostedExtractorSourceYAML
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Sources/operation/create_sources"
    _SupportedSources = (MQTTSourceRequest, KafkaSourceRequest, RESTSourceRequest, EventHubSourceRequest)

    @property
    def display_name(self) -> str:
        return "hosted extractor sources"

    @classmethod
    def get_id(cls, item: HostedExtractorSourceRequestUnion | HostedExtractorSourceResponseUnion | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[HostedExtractorSourceRequestUnion] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [HostedExtractorsAcl.Action.Read]
            if read_only
            else [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write]
        )

        return HostedExtractorsAcl(
            actions,
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: Sequence[HostedExtractorSourceRequestUnion]) -> list[HostedExtractorSourceResponseUnion]:
        return self.client.tool.hosted_extractors.sources.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[HostedExtractorSourceResponseUnion]:
        return self.client.tool.hosted_extractors.sources.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[HostedExtractorSourceRequestUnion]) -> list[HostedExtractorSourceResponseUnion]:
        return self.client.tool.hosted_extractors.sources.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.hosted_extractors.sources.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[HostedExtractorSourceResponseUnion]:
        for sources in self.client.tool.hosted_extractors.sources.iterate():
            yield from sources

    def dump_resource(
        self, resource: HostedExtractorSourceResponseUnion, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        HighSeverityWarning(
            "Sources will always be considered different, and thus will always be redeployed."
        ).print_warning()
        return self.dump_id(self.get_id(resource))

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
        | HTTPBasicAuthenticationRequest,
    ) -> Iterable[str]:
        if isinstance(auth, BasicAuthenticationRequest | ScramShaAuthenticationRequest) and auth.password:
            yield auth.password
        elif isinstance(auth, ClientCredentialAuthenticationRequest):
            yield auth.client_secret
        elif isinstance(auth, HTTPBasicAuthenticationRequest):
            yield auth.value


class HostedExtractorDestinationCRUD(
    ResourceCRUD[ExternalId, HostedExtractorDestinationRequest, HostedExtractorDestinationResponse]
):
    folder_name = "hosted_extractors"
    resource_cls = HostedExtractorDestinationResponse
    resource_write_cls = HostedExtractorDestinationRequest
    dependencies = frozenset({DataSetsCRUD})
    kind = "Destination"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Destinations/operation/create_destinations"
    yaml_cls = HostedExtractorDestinationYAML

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None = None):
        super().__init__(client, build_dir, console)
        self._authentication_by_id: dict[str, ClientCredentials] = {}

    @property
    def display_name(self) -> str:
        return "hosted extractor destinations"

    @classmethod
    def get_id(cls, item: HostedExtractorDestinationRequest | HostedExtractorDestinationResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[HostedExtractorDestinationRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [HostedExtractorsAcl.Action.Read]
            if read_only
            else [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write]
        )

        return HostedExtractorsAcl(
            actions,
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: Sequence[HostedExtractorDestinationRequest]) -> list[HostedExtractorDestinationResponse]:
        return self.client.tool.hosted_extractors.destinations.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[HostedExtractorDestinationResponse]:
        return self.client.tool.hosted_extractors.destinations.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[HostedExtractorDestinationRequest]) -> list[HostedExtractorDestinationResponse]:
        return self.client.tool.hosted_extractors.destinations.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.hosted_extractors.destinations.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[HostedExtractorDestinationResponse]:
        for destinations in self.client.tool.hosted_extractors.destinations.iterate():
            yield from destinations

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> HostedExtractorDestinationRequest:
        if raw_auth := resource.pop("credentials", None):
            credentials = ClientCredentials._load(raw_auth)
            self._authentication_by_id[self.get_id(resource).external_id] = credentials
            if is_dry_run:
                resource["credentials"] = {"nonce": "dummy_nonce"}
            else:
                session = self.client.iam.sessions.create(credentials, "CLIENT_CREDENTIALS")
                resource["credentials"] = {"nonce": session.nonce}
        if ds_external_id := resource.pop("targetDataSetExternalId", None):
            resource["targetDataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return HostedExtractorDestinationRequest.model_validate(resource)

    def dump_resource(
        self, resource: HostedExtractorDestinationResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        HighSeverityWarning(
            "Destinations will always be considered different, and thus will always be redeployed."
        ).print_warning()
        return self.dump_id(self.get_id(resource))

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "targetDataSetId" in item:
            yield DataSetsCRUD, ExternalId(external_id=item["targetDataSetId"])

    def sensitive_strings(self, item: HostedExtractorDestinationRequest) -> Iterable[str]:
        if item.credentials:
            yield item.credentials.nonce
        id_ = self.get_id(item)
        if id_.external_id in self._authentication_by_id:
            yield self._authentication_by_id[id_.external_id].client_secret


class HostedExtractorJobCRUD(ResourceCRUD[ExternalId, HostedExtractorJobRequest, HostedExtractorJobResponse]):
    folder_name = "hosted_extractors"
    resource_cls = HostedExtractorJobResponse
    resource_write_cls = HostedExtractorJobRequest
    dependencies = frozenset({HostedExtractorSourceCRUD, HostedExtractorDestinationCRUD})
    kind = "Job"
    yaml_cls = HostedExtractorJobYAML
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Jobs/operation/create_jobs"

    @property
    def display_name(self) -> str:
        return "hosted extractor jobs"

    @classmethod
    def get_id(cls, item: HostedExtractorJobRequest | HostedExtractorJobResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[HostedExtractorJobRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [HostedExtractorsAcl.Action.Read]
            if read_only
            else [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write]
        )

        return HostedExtractorsAcl(
            actions,
            HostedExtractorsAcl.Scope.All(),
        )

    def dump_resource(
        self, resource: HostedExtractorJobResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        local = local or {}
        if not dumped.get("config") and "config" not in local:
            dumped.pop("config", None)
        return dumped

    def create(self, items: Sequence[HostedExtractorJobRequest]) -> list[HostedExtractorJobResponse]:
        return self.client.tool.hosted_extractors.jobs.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[HostedExtractorJobResponse]:
        return self.client.tool.hosted_extractors.jobs.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[HostedExtractorJobRequest]) -> list[HostedExtractorJobResponse]:
        return self.client.tool.hosted_extractors.jobs.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.hosted_extractors.jobs.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[HostedExtractorJobResponse]:
        for jobs in self.client.tool.hosted_extractors.jobs.iterate():
            yield from jobs

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "sourceId" in item:
            yield HostedExtractorSourceCRUD, ExternalId(external_id=item["sourceId"])
        if "destinationId" in item:
            yield HostedExtractorDestinationCRUD, ExternalId(external_id=item["destinationId"])


class HostedExtractorMappingCRUD(
    ResourceCRUD[ExternalId, HostedExtractorMappingRequest, HostedExtractorMappingResponse]
):
    folder_name = "hosted_extractors"
    resource_cls = HostedExtractorMappingResponse
    resource_write_cls = HostedExtractorMappingRequest
    # This is not an explicit dependency, however, adding it here as mapping will should be deployed after source.
    dependencies = frozenset({HostedExtractorSourceCRUD})
    kind = "Mapping"
    _doc_base_url = "https://api-docs.cognite.com/20230101-alpha/tag/"
    _doc_url = "Mappings/operation/create_mappings"
    yaml_cls = HostedExtractorMappingYAML

    @property
    def display_name(self) -> str:
        return "hosted extractor mappings"

    @classmethod
    def get_id(cls, item: HostedExtractorMappingRequest | HostedExtractorMappingResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[HostedExtractorMappingRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [HostedExtractorsAcl.Action.Read]
            if read_only
            else [HostedExtractorsAcl.Action.Read, HostedExtractorsAcl.Action.Write]
        )

        return HostedExtractorsAcl(
            actions,
            HostedExtractorsAcl.Scope.All(),
        )

    def create(self, items: Sequence[HostedExtractorMappingRequest]) -> list[HostedExtractorMappingResponse]:
        return self.client.tool.hosted_extractors.mappings.create(list(items))

    def retrieve(self, ids: Sequence[ExternalId]) -> list[HostedExtractorMappingResponse]:
        return self.client.tool.hosted_extractors.mappings.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[HostedExtractorMappingRequest]) -> list[HostedExtractorMappingResponse]:
        return self.client.tool.hosted_extractors.mappings.update(list(items), mode="replace")

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.hosted_extractors.mappings.delete(list(ids), ignore_unknown_ids=True)
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[HostedExtractorMappingResponse]:
        for mappings in self.client.tool.hosted_extractors.mappings.iterate():
            yield from mappings
