import httpx
import pytest
import responses
import respx
from cognite.client.data_classes import Annotation, AnnotationList

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.storageio import AnnotationIO
from cognite_toolkit._cdf_tk.storageio.selectors import DataSetSelector


class TestAnnotationIO:
    @pytest.mark.usefixtures("disable_pypi_check", "disable_gzip")
    def test_stream_file_annotations(
        self, toolkit_config: ToolkitClientConfig, rsps: responses.RequestsMock, respx_mock: respx.MockRouter
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config)
        respx_mock.post(config.create_api_url("/files/list")).mock(
            return_value=httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": 1,
                            "externalId": "external_1",
                            "name": "file_1",
                            "createdTime": 0,
                            "lastUpdatedTime": 0,
                            "uploaded": True,
                        },
                    ]
                },
            )
        )
        rsps.add(
            responses.POST,
            config.create_api_url("/files/byids"),
            json={
                "items": [
                    {"id": 1, "externalId": "external_1", "name": "file_1"},
                    {"id": 2, "externalId": "external_2", "name": "file_2"},
                ]
            },
            status=200,
        )
        rsps.add(
            responses.POST,
            config.create_api_url("/assets/byids"),
            json={
                "items": [
                    {"id": 3, "externalId": "external_3", "name": "asset_3"},
                ]
            },
            status=200,
        )
        annotations = AnnotationList(
            [
                Annotation(
                    annotation_type="diagrams.FileLink",
                    status="approved",
                    creating_app="test_app",
                    creating_app_version="v1",
                    annotated_resource_id=1,
                    annotated_resource_type="file",
                    creating_user="test_user",
                    data={
                        "fileRef": {
                            "id": 2,
                        },
                        "textRegion": {
                            "xMin": 0,
                            "xMax": 100,
                            "yMin": 0,
                            "yMax": 100,
                        },
                    },
                ),
                Annotation(
                    annotation_type="diagrams.AssetLink",
                    status="approved",
                    creating_app="test_app",
                    creating_app_version="v1",
                    annotated_resource_id=1,
                    annotated_resource_type="file",
                    creating_user="test_user",
                    data={
                        "assetRef": {"id": 3},
                        "textRegion": {
                            "xMin": 10,
                            "xMax": 110,
                            "yMin": 10,
                            "yMax": 110,
                        },
                    },
                ),
            ]
        )
        for annotation in annotations:
            rsps.add(
                responses.POST,
                config.create_api_url("/annotations/list"),
                json={"items": [annotation.dump()]},
                status=200,
            )

        annotation_io = AnnotationIO(client)
        selector = DataSetSelector(kind="FileMetadata", data_set_external_id="test_data_set")

        pages = list(annotation_io.stream_data(selector))
        assert len(pages) == 2
        total_items = sum(len(page.items) for page in pages)
        assert total_items == 2
        assert annotation_io.count(selector) is None

        json_data = [data for page in pages for data in annotation_io.data_to_json_chunk(page.items)]
        assert len(json_data) == 2
        assert json_data == [
            {
                "annotatedResourceExternalId": "external_1",
                "annotatedResourceType": "file",
                "annotationType": "diagrams.FileLink",
                "creatingApp": "test_app",
                "creatingAppVersion": "v1",
                "creatingUser": "test_user",
                "data": {
                    "fileRef": {"externalId": "external_2"},
                    "textRegion": {"xMax": 100, "xMin": 0, "yMax": 100, "yMin": 0},
                },
                "status": "approved",
            },
            {
                "annotatedResourceExternalId": "external_1",
                "annotatedResourceType": "file",
                "annotationType": "diagrams.AssetLink",
                "creatingApp": "test_app",
                "creatingAppVersion": "v1",
                "creatingUser": "test_user",
                "data": {
                    "assetRef": {"externalId": "external_3"},
                    "textRegion": {"xMax": 110, "xMin": 10, "yMax": 110, "yMin": 10},
                },
                "status": "approved",
            },
        ]
