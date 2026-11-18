from typing import Any

from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_preview import (
    HostedExtractorPreviewRequest,
    HostedExtractorPreviewResponse,
    HostedExtractorPreviewResult,
)

_PREVIEW_RESULT_ADAPTER: TypeAdapter[HostedExtractorPreviewResult] = TypeAdapter(HostedExtractorPreviewResult)


class HostedExtractorPreviewsAPI:
    """Test a source by reading a message from it, without creating a job.

    A preview runs in the background: create it, then poll ``result`` until it is no longer pending.
    """

    def __init__(self, http_client: HTTPClient) -> None:
        self._http_client = http_client

    def _post(self, path: str, body: dict[str, Any], accept: str = "application/json") -> SuccessResponse:
        request = RequestMessage(
            endpoint_url=self._http_client.config.create_api_url(path),
            method="POST",
            body_content=body,
            accept=accept,
        )
        return self._http_client.request_single_retries(request).get_success_or_raise(request)

    def create(self, item: HostedExtractorPreviewRequest) -> HostedExtractorPreviewResponse:
        """`Start a preview. <https://api-docs.cognite.com/20230101/tag/Previews/operation/create_preview>`_"""
        response = self._post("/hostedextractors/preview", item.dump())
        return HostedExtractorPreviewResponse.model_validate_json(response.body)

    def result(self, external_id: str) -> HostedExtractorPreviewResult:
        """`Get the result of a preview. <https://api-docs.cognite.com/20230101/tag/Previews/operation/get_preview_result>`_

        Returns:
            The parsed message, an error, or a pending marker if the preview has not finished.
        """
        response = self._post("/hostedextractors/preview/result", {"externalId": external_id})
        return _PREVIEW_RESULT_ADAPTER.validate_json(response.body)

    def download(self, external_id: str) -> bytes:
        """`Download the raw message read by a preview. <https://api-docs.cognite.com/20230101/tag/Previews/operation/download_preview_payload>`_"""
        return self._post(
            "/hostedextractors/preview/download", {"externalId": external_id}, accept="application/binary"
        ).content
