from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from cognite.client import _version as CogniteSDKVersion
from packaging.requirements import Requirement

from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.streamlit_ import StreamlitRequest
from cognite_toolkit._cdf_tk.resource_ios._resource_ios.industrial_tool import StreamlitIO
from cognite_toolkit._cdf_tk.tk_warnings import StreamlitRequirementsWarning


class TestStreamlitLoader:
    @pytest.fixture
    def recommended_packages(self):
        return StreamlitIO.recommended_packages()

    def test_requirements(self, recommended_packages):
        assert Requirement("pyodide-http==0.2.1") in recommended_packages
        assert Requirement(f"cognite-sdk=={CogniteSDKVersion.__version__}") in recommended_packages

    def test_requirements_to_str(self, recommended_packages):
        assert set([str(r) for r in recommended_packages]) == set(
            ["pyodide-http==0.2.1", f"cognite-sdk=={CogniteSDKVersion.__version__}"]
        )

    def user_requirements_txt() -> list[tuple[list[str], StreamlitRequirementsWarning]]:
        return [
            (["pyodide-http==0.2.1", "cognite-sdk==7.62.1"], []),
            (["pyodide-http==0.0.0", "cognite-sdk", "numpy"], []),
            (["pyodide-http", "numpy"], ["cognite-sdk"]),
            (["numpy"], ["pyodide-http", "cognite-sdk"]),
            (["numpy", "pandas"], ["pyodide-http", "cognite-sdk"]),
            ([], ["pyodide-http", "cognite-sdk"]),
        ]

    @pytest.mark.parametrize(
        "mock_content, expected_result",
        user_requirements_txt(),
    )
    def test_requirements_file_missing_packages(self, mock_content, expected_result) -> None:
        actual = StreamlitIO._missing_recommended_requirements(mock_content)
        assert set(actual) == set(expected_result)


class TestStreamlitUpdateWithFileio:
    def _file_response(self, external_id: str, app_hash: str | None) -> FileMetadataResponse:
        return FileMetadataResponse(
            external_id=external_id,
            name=f"{external_id}-source.json",
            id=1,
            created_time=0,
            last_updated_time=0,
            uploaded=True,
            metadata={
                "creator": "tester",
                "entrypoint": "app.py",
                **({StreamlitIO._metadata_hash_key: app_hash} if app_hash else {}),
            },
        )

    @pytest.mark.parametrize(
        "cdf_hash, local_hash, expect_upload",
        [
            pytest.param("old", "new", True, id="hash_changed_triggers_reupload"),
            pytest.param("same", "same", False, id="hash_unchanged_skips_reupload"),
        ],
    )
    def test_reupload_based_on_hash(self, cdf_hash: str, local_hash: str, expect_upload: bool) -> None:
        mock_client = MagicMock()
        loader = StreamlitIO(mock_client, None, None, use_fileio=True)
        ext_id = "stapp-test"
        loader.filemetadata_by_external_id[ext_id] = MagicMock(spec=Path)

        item = StreamlitRequest(
            external_id=ext_id,
            name="Test App",
            creator="tester",
            entrypoint="app.py",
            cognite_toolkit_app_hash=local_hash,
        )
        source_file = MagicMock(spec=Path)
        file_request = MagicMock(external_id=ext_id, filepath=source_file)
        file_request.as_id.return_value = MagicMock(external_id=ext_id)

        with patch("cognite_toolkit._cdf_tk.resource_ios._resource_ios.industrial_tool.FileMetadataCRUD") as MockCRUD:
            mock_fileio = MagicMock()
            MockCRUD.return_value = mock_fileio
            mock_fileio.load_resource_files.return_value = [file_request]
            mock_fileio.retrieve.return_value = [self._file_response(ext_id, cdf_hash)]
            mock_fileio.update.return_value = [self._file_response(ext_id, local_hash)]
            mock_client.tool.filemetadata.get_upload_url.return_value = [
                MagicMock(upload_url="https://upload.example.com")
            ]
            loader._update_with_fileio([item])

        assert mock_client.tool.filemetadata.upload_file.called == expect_upload
