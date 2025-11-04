from cognite.client.data_classes import Annotation, AnnotationList, FileMetadata

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.storageio import FileAnnotationIO
from cognite_toolkit._cdf_tk.storageio.selectors import DataSetSelector
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestAnnotationIO:
    def test_stream_file_annotations(self) -> None:
        with monkeypatch_toolkit_client() as toolkit_client:
            # Allow lookup of internal IDs to external IDs
            approval_client = ApprovalToolkitClient(toolkit_client, allow_reverse_lookup=True)
            approval_client.mock_client.files.return_value = [[FileMetadata(external_id="file1", id=1)]]
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
            approval_client.mock_client.annotations.list.return_value = annotations

            annotation_io = FileAnnotationIO(approval_client.client)
            selector = DataSetSelector(kind="FileMetadata", data_set_external_id="test_data_set")

            pages = list(annotation_io.stream_data(selector))
            assert len(pages) == 1
            total_items = sum(len(page.items) for page in pages)
            assert total_items == 2
            assert annotation_io.count(selector) is None

            json_data = annotation_io.data_to_json_chunk(pages[0].items)
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
