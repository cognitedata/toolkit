import gzip
import random
import time
from collections.abc import MutableMapping

import requests
from cognite.client import ClientConfig, CogniteClient, global_config
from cognite.client._api.functions import FunctionsAPI
from cognite.client._http_client import HTTPClient, HTTPClientConfig, get_global_requests_session
from cognite.client.data_classes import Function, FunctionWrite
from cognite.client.utils import _json
from cognite.client.utils._auxiliary import get_user_agent
from requests.structures import CaseInsensitiveDict
from rich.console import Console

from cognite_toolkit._cdf_tk.tk_warnings import HighSeverityWarning
from cognite_toolkit._cdf_tk.utils._auxiliary import get_current_toolkit_version
from cognite_toolkit._cdf_tk.utils.useful_types import JsonVal


class ExtendedFunctionsAPI(FunctionsAPI):
    def __init__(
        self,
        config: ClientConfig,
        api_version: str | None,
        cognite_client: CogniteClient,
    ) -> None:
        """
        Extended Functions API to include custom headers and payload preparation.
        """
        super().__init__(config, api_version, cognite_client)
        session = get_global_requests_session()
        # The HTTPClient in the parent class always retries 429 responses, but we want to handle that manually.
        self._http_client_no_retry = HTTPClient(
            config=HTTPClientConfig(
                status_codes_to_retry=set(),
                backoff_factor=0.5,
                max_backoff_seconds=global_config.max_retry_backoff,
                max_retries_total=global_config.max_retries,
                max_retries_read=0,
                max_retries_connect=global_config.max_retries_connect,
                max_retries_status=0,
            ),
            session=session,
            refresh_auth_header=self._refresh_auth_header,
        )

    def create_with_429_retry(self, function: FunctionWrite, console: Console | None = None) -> Function:
        """Create a function with manual retry handling for 429 Too Many Requests responses.

        This method is a workaround for scenarios where the function creation API is temporarily unavailable
        due to a full queue on the server side.

        Args:
            function (FunctionWrite): The function to create.
            console (Console | None): The rich console to use for printing warnings.

        Returns:
            Function: The created function object.
        """
        headers = self._create_headers()
        payload = self._prepare_payload({"items": [function.dump(camel_case=True)]})
        _, full_url = self._resolve_url("POST", self._RESOURCE_PATH)
        retry_count = 0
        while True:
            response = self._http_client_no_retry.request(
                method="POST",
                url=full_url,
                headers=headers,
                data=payload,
                timeout=self._config.timeout,
                allow_redirects=False,
            )

            match response.status_code:
                case 200 | 201 | 202 | 204:
                    return Function._load(response.json()["items"][0], cognite_client=self._cognite_client)
                case 401:
                    self._raise_no_project_access_error(response)
                case 429 if retry_count < global_config.max_retries:
                    try:
                        retry_after = float(response.headers.get("Retry-After", 60))
                    except ValueError:
                        retry_after = 60.0 + random.uniform(-5, 5)
                    HighSeverityWarning(f"Rate limit exceeded. Retrying after {retry_after} seconds.").print_warning(
                        console=console
                    )
                    time.sleep(retry_after)
                    retry_count += 1
                    continue
                case _:
                    self._raise_api_error(response, {"items": function.dump(camel_case=True)})

    def _create_headers(self) -> MutableMapping[str, str]:
        headers: MutableMapping[str, str] = CaseInsensitiveDict()
        headers.update(requests.utils.default_headers())
        auth_name, auth_value = self._config.credentials.authorization_header()
        headers[auth_name] = auth_value
        headers["content-type"] = "application/json"
        headers["accept"] = "application/json"
        headers["x-cdp-sdk"] = f"CogniteToolkit:{get_current_toolkit_version()}"
        headers["x-cdp-app"] = self._config.client_name
        headers["cdf-version"] = self._config.api_subversion
        if "User-Agent" in headers:
            headers["User-Agent"] += f" {get_user_agent()}"
        else:
            headers["User-Agent"] = get_user_agent()
        if not global_config.disable_gzip:
            headers["Content-Encoding"] = "gzip"
        return headers

    @staticmethod
    def _prepare_payload(body: JsonVal) -> str | bytes:
        """
        Prepare the payload for the HTTP request.
        This method should be overridden in subclasses to customize the payload format.
        """
        data: str | bytes
        try:
            data = _json.dumps(body, allow_nan=False)
        except ValueError as e:
            # A lot of work to give a more human friendly error message when nans and infs are present:
            msg = "Out of range float values are not JSON compliant"
            if msg in str(e):  # exc. might e.g. contain an extra ": nan", depending on build (_json.make_encoder)
                raise ValueError(f"{msg}. Make sure your data does not contain NaN(s) or +/- Inf!").with_traceback(
                    e.__traceback__
                ) from None
            raise

        if not global_config.disable_gzip:
            data = gzip.compress(data.encode())
        return data
