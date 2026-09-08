from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    CogniteFormat,
    HostedExtractorJobRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    BasicAuthenticationRequest,
    EventHubSourceRequest,
)
from tests.test_integration.constants import RUN_UNIQUE_ID


class TestHostedExtractorJobsAPI:
    def test_update_eventhub_job_without_config(self, toolkit_client: ToolkitClient) -> None:
        """Event Hub jobs have no source-specific config.

        Redeploy uses replace-mode update. CDF rejects ``config.setNull``, which is
        what toolkit previously emitted when ``config`` was None.
        """
        client = toolkit_client
        suffix = RUN_UNIQUE_ID.lower()
        source_id = ExternalId(external_id=f"toolkit_test_eventhub_source_{suffix}")
        dest_id = ExternalId(external_id=f"toolkit_test_eventhub_dest_{suffix}")
        job_id = ExternalId(external_id=f"toolkit_test_eventhub_job_{suffix}")

        source = EventHubSourceRequest(
            external_id=source_id.external_id,
            host="toolkit-test.servicebus.windows.net",
            event_hub_name="toolkit-test-hub",
            authentication=BasicAuthenticationRequest(username="toolkit-test-key", password="not-a-real-secret"),
        )
        destination = HostedExtractorDestinationRequest(external_id=dest_id.external_id)
        job = HostedExtractorJobRequest(
            external_id=job_id.external_id,
            source_id=source_id.external_id,
            destination_id=dest_id.external_id,
            format=CogniteFormat(),
        )
        assert job.config is None
        assert "config" not in job.as_update(mode="replace")["update"], "Config should not be set to null."

        try:
            created_sources = client.tool.hosted_extractors.sources.create([source])
            assert len(created_sources) == 1
            created_destinations = client.tool.hosted_extractors.destinations.create([destination])
            assert len(created_destinations) == 1
            created_jobs = client.tool.hosted_extractors.jobs.create([job])
            assert len(created_jobs) == 1
            assert created_jobs[0].external_id == job.external_id

            updated = client.tool.hosted_extractors.jobs.update([job], mode="replace")
            assert len(updated) == 1
            assert updated[0].external_id == job.external_id
            assert updated[0].source_id == source_id.external_id
            assert updated[0].destination_id == dest_id.external_id
        finally:
            client.tool.hosted_extractors.jobs.delete([job_id], ignore_unknown_ids=True)
            client.tool.hosted_extractors.destinations.delete([dest_id], ignore_unknown_ids=True, force=True)
            client.tool.hosted_extractors.sources.delete([source_id], ignore_unknown_ids=True, force=True)
