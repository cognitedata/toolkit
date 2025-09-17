import pytest
from cognite.client import _version as CogniteSDKVersion
from packaging.requirements import Requirement

from cognite_toolkit._cdf_tk.cruds._resource_cruds.industrial_tool import StreamlitCRUD
from cognite_toolkit._cdf_tk.tk_warnings import StreamlitRequirementsWarning


class TestStreamlitLoader:
    @pytest.fixture
    def recommended_packages(self):
        return StreamlitCRUD.recommended_packages()

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
        actual = StreamlitCRUD._missing_recommended_requirements(mock_content)
        assert set(actual) == set(expected_result)
