import gzip
from typing import Any

from cognite.client._api.transformations import TransformationsAPI
from cognite.client.config import global_config
from cognite.client.data_classes import TransformationPreviewResult
from cognite.client.utils import _json
from requests import Response


class FixedTransformationsAPI(TransformationsAPI):
    """This overrides the preview method in the transformations API to allow for longer timeouts and retries."""

    _default_timeout_run_query = 240  # seconds, this is the maximum timeout for running queries in CDF

    def preview(
        self,
        query: str | None = None,
        convert_to_string: bool = False,
        limit: int | None = 100,
        source_limit: int | None = 100,
        infer_schema_limit: int | None = 10_000,
        timeout: int | None = _default_timeout_run_query,
    ) -> TransformationPreviewResult:
        """`Preview the result of a query. <https://developer.cognite.com/api#tag/Query/operation/runPreview>`_

        This method overrides the default preview method in the TransformationsAPI to allow for longer
        timeouts and retries.

        Toolkit runs long-running queries that takes longer than the typical default of 30 seconds. In addition,
        we do not want to retry, which typically up to 10 times, as the user will have to wait for a long time. Instead,
        we want to fail provide the user with the error and then let the user decide whether to retry or not by
        running the CLI command again.

        Args:
            query (str | None): SQL query to run for preview.
            convert_to_string (bool): Stringify values in the query results, default is False.
            limit (int | None): Maximum number of rows to return in the final result, default is 100.
            source_limit (int | None): Maximum number of items to read from the data source or None to run without limit, default is 100.
            infer_schema_limit (int | None): Limit for how many rows that are used for inferring result schema, default is 10 000.
            timeout (int | None): Number of seconds to wait before cancelling a query. The default, and maximum, is 240.

        Returns:
            TransformationPreviewResult: Result of the executed query
        """
        request_body = {
            "query": query,
            "convertToString": convert_to_string,
            "limit": limit,
            "sourceLimit": source_limit,
            "inferSchemaLimit": infer_schema_limit,
            "timeout": timeout,
        }
        response = self._do_requests_retry(
            "POST",
            "/transformations/query/run",
            json=request_body,
            timeout=timeout or (self._default_timeout_run_query + 60),  # add a buffer to the timeout
            retry=False,
        )
        return TransformationPreviewResult._load(response.json(), cognite_client=self._cognite_client)

    def _do_requests_retry(
        self,
        method: str,
        url_path: str,
        accept: str = "application/json",
        api_subversion: str | None = None,
        retry: bool = True,
        **kwargs: Any,
    ) -> Response:
        """This is a copy of the _do_requests method from the APIClient with the option to turn off retries."""
        is_retryable, full_url = self._resolve_url(method, url_path)
        json_payload = kwargs.pop("json", None)
        headers = self._configure_headers(
            accept,
            additional_headers=self._config.headers.copy(),
            api_subversion=api_subversion,
        )
        headers.update(kwargs.get("headers") or {})

        if json_payload is not None:
            try:
                data = _json.dumps(json_payload, allow_nan=False)
            except ValueError as e:
                # A lot of work to give a more human friendly error message when nans and infs are present:
                msg = "Out of range float values are not JSON compliant"
                if msg in str(e):  # exc. might e.g. contain an extra ": nan", depending on build (_json.make_encoder)
                    raise ValueError(f"{msg}. Make sure your data does not contain NaN(s) or +/- Inf!").with_traceback(
                        e.__traceback__
                    ) from None
                raise
            kwargs["data"] = data
            if method in ["PUT", "POST"] and not global_config.disable_gzip:
                kwargs["data"] = gzip.compress(data.encode())
                headers["Content-Encoding"] = "gzip"

        kwargs["headers"] = headers

        # requests will by default follow redirects. This can be an SSRF-hazard if
        # the client can be tricked to request something with an open redirect, in
        # addition to leaking the token, as requests will send the headers to the
        # redirected-to endpoint.
        # If redirects are to be followed in a call, this should be opted into instead.
        kwargs.setdefault("allow_redirects", False)

        if is_retryable and retry:
            res = self._http_client_with_retry.request(method=method, url=full_url, **kwargs)
        else:
            res = self._http_client.request(method=method, url=full_url, **kwargs)

        match res.status_code:
            case 200 | 201 | 202 | 204:
                pass
            case 401:
                self._raise_no_project_access_error(res)
            case _:
                self._raise_api_error(res, payload=json_payload)

        stream = kwargs.get("stream")
        self._log_request(res, payload=json_payload, stream=stream)
        return res
