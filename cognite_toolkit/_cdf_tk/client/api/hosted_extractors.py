from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

from .hosted_extractor_destinations import HostedExtractorDestinationsAPI
from .hosted_extractor_jobs import HostedExtractorJobsAPI
from .hosted_extractor_mappings import HostedExtractorMappingsAPI
from .hosted_extractor_sources import HostedExtractorSourcesAPI


class HostedExtractorsAPI:
    """API for Hosted Extractors resources.

    This class groups all hosted extractor related APIs:
    - sources: Manage hosted extractor sources (MQTT, Kafka, EventHub, REST)
    - jobs: Manage hosted extractor jobs
    - destinations: Manage hosted extractor destinations
    - mappings: Manage hosted extractor mappings
    """

    def __init__(self, http_client: HTTPClient) -> None:
        self.sources = HostedExtractorSourcesAPI(http_client)
        self.jobs = HostedExtractorJobsAPI(http_client)
        self.destinations = HostedExtractorDestinationsAPI(http_client)
        self.mappings = HostedExtractorMappingsAPI(http_client)
