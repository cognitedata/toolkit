from collections import Counter
from collections.abc import Iterable, Sequence
from typing import Self

from pydantic import BaseModel, Field, ValidationError
from pydantic.alias_generators import to_snake

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.limits import LimitValueResponse
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamResponse
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    Insight,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeploymentResult
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import StreamIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitGlobalRuleSet

_STREAMS_LIMIT_PREFIX = "streams."
_TEMPLATE_PREFIX = "template."
_MAX_ACTIVE_STREAMS = "max_active_streams"
_STREAMS_ENDPOINT = "/streams"
# The deployment result resource name, which is the StreamIO display name.
_STREAMS_RESOURCE_NAME = "streams"


class StreamLimits(BaseModel):
    """The ``streams.*`` project limits from the limits API."""

    max_active_streams: int
    # Keyed by the snake case template name, for example, 'basic_live_data' for the 'BasicLiveData' template.
    max_active_streams_by_template: dict[str, int] = Field(default_factory=dict)

    @classmethod
    def from_responses(cls, limits: Sequence[LimitValueResponse]) -> Self:
        values: dict[str, float] = {}
        by_template: dict[str, float] = {}
        template_suffix = f".{_MAX_ACTIVE_STREAMS}"
        for item in limits:
            name = item.limit_id.removeprefix(_STREAMS_LIMIT_PREFIX)
            if name.startswith(_TEMPLATE_PREFIX) and name.endswith(template_suffix):
                by_template[name.removeprefix(_TEMPLATE_PREFIX).removesuffix(template_suffix)] = item.value
            else:
                values[name] = item.value
        # Pydantic accepts whole floats such as 3.0 for the int fields and rejects fractional ones.
        return cls.model_validate({**values, "max_active_streams_by_template": by_template})

    def template_limit(self, template: str) -> int | None:
        return self.max_active_streams_by_template.get(to_snake(template))


def _streams_phrase(count: int) -> str:
    return f"{count} stream" if count == 1 else f"{count} streams"


class StreamRuleSet(ToolkitGlobalRuleSet):
    """Validates that the project has capacity for the streams that deploying would create.

    This requires deployment results, as they are the only way to know which streams would be created and deleted.
    """

    CODE_PREFIX = "STREAM"
    DISPLAY_NAME = "Streams"

    def get_status(self) -> RuleSetStatus:
        if self.client is None or not self.results:
            return RuleSetStatus(
                code="skip",
                message=(
                    "Stream capacity validation requires a client and deployment results. "
                    "Run as part of a deploy to check that the project has room for new streams."
                ),
            )
        return RuleSetStatus(
            code="ready",
            message="Will validate that the project has capacity for the streams to be created.",
        )

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None or not self.results:
            return
        created_ids, deleted_ids = self._deployed_stream_ids(self.results)
        if not created_ids:
            return
        try:
            limits = StreamLimits.from_responses(self.client.limits.list(prefix=_STREAMS_LIMIT_PREFIX, limit=None))
            existing = self.client.streams.list()
        except ToolkitAPIError as error:
            yield InternalValidatorException(
                message=f"Failed to retrieve stream limits or streams: {error}", source="Streams"
            )
            return
        except ValidationError as error:
            yield InternalValidatorException(message=f"Failed to parse stream limits: {error}", source="Streams")
            return
        deleted_existing = [stream for stream in existing if stream.as_id() in deleted_ids]
        endpoint_url = self.client.config.create_api_url(_STREAMS_ENDPOINT)
        yield from self._check_capacity(
            "active streams",
            current=len(existing),
            created=len(created_ids),
            deleted=len(deleted_existing),
            limit=limits.max_active_streams,
            endpoint_url=endpoint_url,
        )
        yield from self._check_template_capacity(
            limits, existing, deleted_existing, self._local_templates(self.client, created_ids), endpoint_url
        )

    @staticmethod
    def _deployed_stream_ids(results: Sequence[DeploymentResult]) -> tuple[list[ExternalId], set[ExternalId]]:
        created: list[ExternalId] = []
        deleted: set[ExternalId] = set()
        for result in results:
            if result.resource_name != _STREAMS_RESOURCE_NAME:
                continue
            created.extend(item for item in result.created_ids if isinstance(item, ExternalId))
            deleted.update(item for item in result.deleted_ids if isinstance(item, ExternalId))
        return created, deleted

    def _local_templates(self, client: ToolkitClient, created_ids: Sequence[ExternalId]) -> list[str]:
        """The template of each stream that deploying would create, as configured locally."""
        created = set(created_ids)
        return [
            validatable.request.settings.template.name
            for validatable in self.resources.get_resource_of_type(StreamIO(client))
            if validatable.request.as_id() in created
        ]

    @classmethod
    def _check_template_capacity(
        cls,
        limits: StreamLimits,
        existing: Sequence[StreamResponse],
        deleted_existing: Sequence[StreamResponse],
        created_templates: Sequence[str],
        endpoint_url: str,
    ) -> Iterable[Insight]:
        current_by_template = Counter(stream.created_from_template for stream in existing)
        deleted_by_template = Counter(stream.created_from_template for stream in deleted_existing)
        for template, created_count in Counter(created_templates).items():
            limit = limits.template_limit(template)
            if limit is None:
                continue
            yield from cls._check_capacity(
                f"active '{template}' streams",
                current=current_by_template[template],
                created=created_count,
                deleted=deleted_by_template[template],
                limit=limit,
                endpoint_url=endpoint_url,
            )

    @staticmethod
    def _check_capacity(
        noun: str, current: int, created: int, deleted: int, limit: int, endpoint_url: str
    ) -> Iterable[Insight]:
        projected = current + created - deleted
        if projected <= current or projected <= limit:
            return
        if deleted:
            change = f"create {_streams_phrase(created)} and delete {_streams_phrase(deleted)}, changing"
        else:
            change = f"create {_streams_phrase(created)}, increasing"
        yield BuildError(
            code="EXCEEDED-LIMIT",
            message=(
                f"Deploying would {change} the number of {noun} from {current} to {projected}, "
                f"which exceeds the project limit of {limit}."
            ),
            fix=(
                f"Reduce the number of new streams in this deployment, or delete existing streams, so that "
                f"the number of {noun} stays within {limit}. The project currently has {current} of {limit}."
            ),
            source_url=endpoint_url,
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )
