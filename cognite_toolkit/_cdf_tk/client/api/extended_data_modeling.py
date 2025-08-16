import gzip
import time
from collections.abc import MutableMapping, Sequence
from typing import Any

from cognite.client import global_config
from cognite.client._api.data_modeling import DataModelingAPI
from cognite.client._api.data_modeling.instances import InstancesAPI
from cognite.client._api_client import T
from cognite.client._cognite_client import ClientConfig, CogniteClient
from cognite.client._http_client import HTTPClient, HTTPClientConfig, get_global_requests_session
from cognite.client.data_classes.data_modeling import EdgeId, InstanceApply, NodeId
from cognite.client.exceptions import CogniteConnectionError, CogniteReadTimeout
from cognite.client.utils import _json
from cognite.client.utils._concurrency import execute_tasks
from requests import Response

from cognite_toolkit._cdf_tk.client._constants import DATA_MODELING_MAX_DELETE_WORKERS, DATA_MODELING_MAX_WRITE_WORKERS
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstancesApplyResultList
from cognite_toolkit._cdf_tk.client.utils._concurrency import ToolkitConcurrencySettings
from cognite_toolkit._cdf_tk.client.utils._http_client import ToolkitRetryTracker


class ExtendedDataModelingAPI(DataModelingAPI):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        self.instances: ExtendedInstancesAPI = ExtendedInstancesAPI(config, api_version, cognite_client)


class ExtendedInstancesAPI(InstancesAPI):
    def __init__(self, config: ClientConfig, api_version: str | None, cognite_client: CogniteClient) -> None:
        super().__init__(config, api_version, cognite_client)
        # The self._http_client retries 429 status codes, which is not desired when we want to reduce the number of items
        # sent in each request when a 400 error occurs.
        self._http_client_no_retry = HTTPClient(
            config=HTTPClientConfig(
                status_codes_to_retry=set(),
                backoff_factor=0.5,
                max_backoff_seconds=global_config.max_retry_backoff,
                max_retries_total=0,
                max_retries_read=0,
                max_retries_connect=0,
                max_retries_status=0,
            ),
            session=get_global_requests_session(),
            refresh_auth_header=self._refresh_auth_header,
        )

    def apply_fast(
        self,
        items: Sequence[InstanceApply],
        auto_create_start_nodes: bool = False,
        auto_create_end_nodes: bool = False,
        auto_create_direct_relations: bool = True,
        skip_on_version_conflict: bool = False,
        replace: bool = False,
    ) -> InstancesApplyResultList:
        """Add or update (upsert) instances. <https://developer.cognite.com/api#tag/Instances/operation/applyNodeAndEdges>`_

        This is an alternative to the standard apply method that is available in the CDF Python SDK. The .apply
        method has two limitations that this method does not have:
        1. The standard apply method uses a thread pool executor with only one worker, even though the API supports 4.
           This method allows up to 4 workers to be used, which can significantly speed up ingestion of large datasets.
        2. The standard apply method has a generic handling of 400 errors. This method dynamically reduces the number
           of items sent in each request if a 400 error occurs.

        Args:
            items: A sequence of instances to apply. Each item can be a NodeApply or an EdgeApply instance.
            auto_create_start_nodes (bool): Whether to create missing start nodes for edges when ingesting. By default, the start node of an edge must exist before it can be ingested.
            auto_create_end_nodes (bool): Whether to create missing end nodes for edges when ingesting. By default, the end node of an edge must exist before it can be ingested.
            auto_create_direct_relations (bool): Whether to create missing direct relation targets when ingesting.
            skip_on_version_conflict (bool): If existingVersion is specified on any of the nodes/edges in the input, the default behaviour is that the entire ingestion will fail when version conflicts occur. If skipOnVersionConflict is set to true, items with version conflicts will be skipped instead. If no version is specified for nodes/edges, it will do the writing directly.
            replace (bool): How do we behave when a property value exists? Do we replace all matching and existing values with the supplied values (true)? Or should we merge in new values for properties together with the existing values (false)? Note: This setting applies for all nodes or edges specified in the ingestion call.

        Returns:
            InstancesApplyResult: Created instance(s)

        """
        parameters = {
            "autoCreateStartNodes": auto_create_start_nodes,
            "autoCreateEndNodes": auto_create_end_nodes,
            "autoCreateDirectRelations": auto_create_direct_relations,
            "skipOnVersionConflict": skip_on_version_conflict,
            "replace": replace,
        }
        tasks = [
            (self._RESOURCE_PATH, task_items, ToolkitRetryTracker(self._http_client_with_retry.config))
            for task_items in self._prepare_item_chunks(items, self._CREATE_LIMIT, parameters)
        ]
        summary = execute_tasks(
            self._post_with_item_reduction_retry,
            tasks,
            max_workers=min(self._config.max_workers, DATA_MODELING_MAX_WRITE_WORKERS),
            executor=ToolkitConcurrencySettings.get_data_modeling_write_executor(),
        )

        def unwrap_element(el: T) -> InstanceApply | NodeId | EdgeId | T:
            if isinstance(el, dict):
                instance_type = el.get("instanceType")
                if instance_type == "node":
                    return NodeId.load(el)
                elif instance_type == "edge":
                    return EdgeId.load(el)
                else:
                    return InstanceApply._load(el)
            else:
                return el

        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task[1]["items"],
            task_list_element_unwrap_fn=unwrap_element,
        )
        created_resources = summary.joined_results(lambda res: res.json()["items"])

        return InstancesApplyResultList._load(created_resources)

    def delete_fast(
        self,
        instance_ids: Sequence[NodeId | EdgeId],
    ) -> list[NodeId | EdgeId]:
        """`Delete one or more instances <https://developer.cognite.com/api#tag/Instances/operation/deleteBulk>`_

        Args:
            instance_ids (Sequence[NodeId | EdgeId]): A sequence of NodeId or EdgeId instances to delete.

        Returns:
            list[NodeId | EdgeId]: A list of NodeId or EdgeId instances that were successfully deleted. An empty
                list is returned if no instances were deleted.
        """
        tasks = [
            (f"{self._RESOURCE_PATH}/delete", task_items, ToolkitRetryTracker(self._http_client_with_retry.config))
            for task_items in self._prepare_item_chunks(
                [
                    {"space": item.space, "externalId": item.external_id, "instanceType": item._instance_type}
                    for item in instance_ids
                ],
                self._DELETE_LIMIT,
                None,
            )
        ]
        summary = execute_tasks(
            self._post_with_item_reduction_retry,
            tasks,
            max_workers=min(self._config.max_workers, DATA_MODELING_MAX_DELETE_WORKERS),
            executor=ToolkitConcurrencySettings.get_data_modeling_delete_executor(),
        )

        def unwrap_element(el: T) -> InstanceApply | T:
            if isinstance(el, dict) and "instanceType" in el:
                if el["instanceType"] == "node":
                    return NodeId.load(el)
                elif el["instanceType"] == "edge":
                    return EdgeId.load(el)
                return el
            else:
                return el

        summary.raise_compound_exception_if_failed_tasks(
            task_unwrap_fn=lambda task: task[1]["items"],
            task_list_element_unwrap_fn=unwrap_element,
        )
        deleted_resources = summary.joined_results(lambda res: res.json()["items"])
        result: list[NodeId | EdgeId] = []
        for resource in deleted_resources:
            if "instanceType" not in resource:
                raise ValueError("Resource must contain 'instanceType' key.")
            instance_type = resource.get("instanceType")
            if instance_type == "node":
                result.append(NodeId.load(resource))
            elif instance_type == "edge":
                result.append(EdgeId.load(resource))
            else:
                raise TypeError(f"Resource must be a NodeId or EdgeId, not {instance_type}.")
        return result

    def _post_with_item_reduction_retry(
        self,
        url_path: str,
        json_payload: dict[str, Any],
        retry_tracker: ToolkitRetryTracker,
    ) -> Response:
        _, full_url = self._resolve_url("POST", url_path)
        headers = self._configure_headers(
            "application/json",
            additional_headers=self._config.headers.copy(),
        )
        data: str | bytes
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

        if not global_config.disable_gzip:
            data = gzip.compress(data.encode())
            headers["Content-Encoding"] = "gzip"

        try:
            result = self._http_client_no_retry.request(
                "POST", full_url, data=data, headers=headers, timeout=self._config.timeout
            )
        except CogniteReadTimeout as e:
            retry_tracker.read += 1
            if not retry_tracker.should_retry(status_code=None, is_auto_retryable=True):
                raise e
            self._sleep(retry_tracker, headers)
            return self._post_with_item_reduction_retry(url_path, json_payload, retry_tracker)
        except CogniteConnectionError as e:
            retry_tracker.connect += 1
            if not retry_tracker.should_retry(status_code=None, is_auto_retryable=True):
                raise e
            self._sleep(retry_tracker, headers)
            return self._post_with_item_reduction_retry(url_path, json_payload, retry_tracker)

        retry_tracker.status += 1
        match result.status_code:
            case 200 | 201 | 202 | 204:
                pass
            case 401:
                self._raise_no_project_access_error(result)
            case 429 | 502 | 503 | 504:
                items = json_payload["items"]
                if retry_tracker.should_retry(status_code=result.status_code, is_auto_retryable=True):
                    if len(items) == 1:
                        self._sleep(retry_tracker, headers)
                        return self._post_with_item_reduction_retry(url_path, json_payload, retry_tracker)
                    # If we get a 429 or 5xx error, we reduce the number of items in the payload and retry.
                    # This is to avoid overwhelming the API with too many items at once.
                    half = len(items) // 2
                    first, second = json_payload.copy(), json_payload.copy()
                    first["items"] = items[:half]
                    second["items"] = items[half:]
                    self._sleep(retry_tracker, headers)
                    first_result = self._post_with_item_reduction_retry(url_path, first, retry_tracker.copy())
                    second_result = self._post_with_item_reduction_retry(url_path, second, retry_tracker.copy())
                    first_result.json()["items"].extend(second_result.json()["items"])
                    return first_result
                self._raise_api_error(result, payload=json_payload)
            case _:
                self._raise_api_error(result, payload=json_payload)

        self._log_request(result, payload=json_payload)
        return result

    def _sleep(self, retry_tracker: ToolkitRetryTracker, headers: MutableMapping[str, Any]) -> None:
        # During a backoff loop, our credentials might expire, so we check and maybe refresh:
        time.sleep(retry_tracker.get_backoff_time())
        self._refresh_auth_header(headers)
