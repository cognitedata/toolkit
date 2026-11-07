from typing import Any, Literal

from cognite_toolkit._cdf_tk.client.cdf_client import CDFResourceAPI, PagedResponse
from cognite_toolkit._cdf_tk.client.cdf_client.api import Endpoint
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient, ItemsSuccessResponse, RequestMessage, SuccessResponse
from cognite_toolkit._cdf_tk.client.identifiers import DataModelId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import Column

ConflictMode = Literal["abort", "upsert", "update", "delete"]
SchemaType = Literal[
    "assets",
    "timeseries",
    "asset_hierarchy",
    "events",
    "datapoints",
    "string_datapoints",
    "sequences",
    "files",
    "labels",
    "relationships",
    "raw",
    "data_sets",
]


class TransformationSchemaAPI(CDFResourceAPI[Column]):
    """API for retrieving the schema (columns) of transformation destinations."""

    def __init__(self, http_client: HTTPClient) -> None:
        super().__init__(
            http_client=http_client,
            method_endpoint_map={
                "retrieve": Endpoint(method="GET", path="/transformations/schema", item_limit=1),
            },
        )

    def _validate_page_response(self, response: SuccessResponse | ItemsSuccessResponse) -> PagedResponse[Column]:
        return PagedResponse[Column].model_validate_json(response.body)

    def _get_columns(self, path_suffix: str, params: dict[str, Any]) -> list[Column]:
        endpoint = self._method_endpoint_map["retrieve"]
        request = RequestMessage(
            endpoint_url=self._make_url(f"{endpoint.path}/{path_suffix}"),
            method=endpoint.method,
            parameters=self._filter_out_none_values(params),
            api_version=self._api_version,
        )
        response = self._http_client.request_single_retries(request).get_success_or_raise(request)
        return self._validate_page_response(response).items

    def retrieve(self, schema_type: SchemaType | str, conflict_mode: ConflictMode | None = None) -> list[Column]:
        """`Get the schema of a resource type. <https://api-docs.cognite.com/20230101/tag/Schema/operation/getTransformationSchema>`_

        Args:
            schema_type: The destination resource type, for example 'assets' or 'timeseries'.
            conflict_mode: The conflict mode of the transformation.

        Returns:
            The columns of the destination schema.
        """
        return self._get_columns(schema_type, {"conflictMode": conflict_mode})

    def retrieve_sequence_rows(self, external_id: str) -> list[Column]:
        """`Get the schema of a sequence. <https://api-docs.cognite.com/20230101/tag/Schema/operation/getTransformationSequenceRowSchema>`_

        Args:
            external_id: External ID of the sequence.

        Returns:
            The columns of the sequence rows schema.
        """
        return self._get_columns("sequence_rows", {"externalId": external_id})

    def retrieve_view_instances(
        self,
        conflict_mode: ConflictMode,
        view: ViewId | None = None,
        instance_type: Literal["nodes", "edges"] | None = None,
        with_instance_space: bool | None = None,
        is_connection_definition: bool | None = None,
    ) -> list[Column]:
        """`Get the view centric instance schema. <https://api-docs.cognite.com/20230101/tag/Schema/operation/getFlexibleDataModelInstanceSchema>`_

        Args:
            conflict_mode: The conflict mode of the transformation.
            view: The view to write to. Not required if is_connection_definition is True.
            instance_type: Whether the transformation writes nodes or edges.
            with_instance_space: Whether the instance space is set in the transformation config.
            is_connection_definition: Whether the edge is a connection definition.

        Returns:
            The columns of the instance schema.
        """
        return self._get_columns(
            "instances",
            {
                "conflictMode": conflict_mode,
                "viewSpace": view.space if view else None,
                "viewExternalId": view.external_id if view else None,
                "viewVersion": view.version if view else None,
                "instanceType": instance_type,
                "withInstanceSpace": with_instance_space,
                "isConnectionDefinition": is_connection_definition,
            },
        )

    def retrieve_data_model_instances(
        self,
        conflict_mode: ConflictMode,
        data_model: DataModelId,
        type: str,
        relationship_from_type: str | None = None,
    ) -> list[Column]:
        """`Get the data model centric instance schema. <https://api-docs.cognite.com/20230101/tag/Schema/operation/getFlexibleDataModelInstanceSchema>`_

        Args:
            conflict_mode: The conflict mode of the transformation.
            data_model: The data model to write to.
            type: External ID of the view in the data model.
            relationship_from_type: Property identifier of the connection definition in the type.

        Returns:
            The columns of the instance schema.
        """
        return self._get_columns(
            "instances",
            {
                "conflictMode": conflict_mode,
                "dataModelSpace": data_model.space,
                "dataModelExternalId": data_model.external_id,
                "dataModelVersion": data_model.version,
                "type": type,
                "relationshipFromType": relationship_from_type,
            },
        )
