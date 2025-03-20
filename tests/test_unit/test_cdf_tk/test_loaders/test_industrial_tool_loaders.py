from pathlib import Path

import pytest
from cognite.client import _version as CogniteSDKVersion
from packaging.requirements import Requirement

from cognite_toolkit._cdf_tk.commands.build import BuildCommand
from cognite_toolkit._cdf_tk.loaders._resource_loaders.industrial_tool_loaders import StreamlitLoader
from cognite_toolkit._cdf_tk.tk_warnings import StreamlitRequirementsWarning
from cognite_toolkit._cdf_tk.utils.file import safe_read
from tests.data import COMPLETE_ORG


class TestStreamlitLoader:
    @pytest.fixture
    def recommended_packages(self):
        return StreamlitLoader.recommended_packages()

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
        actual = StreamlitLoader._missing_recommended_requirements(mock_content)
        assert set(actual) == set(expected_result)

    @staticmethod
    def mock_safe_read(file_path, mock_content):
        if file_path.name == "requirements.txt":
            return mock_content  # Your mocked content
        else:
            # If it's not requirements.txt, call the original safe_read
            return safe_read(file_path)

    @pytest.mark.parametrize(
        "mock_content, expected_result",
        user_requirements_txt(),
    )
    def test_requirements_file_missing_packages_warnings(
        self, monkeypatch, tmp_path: Path, mock_content, expected_result
    ) -> None:
        monkeypatch.setattr(
            "cognite_toolkit._cdf_tk.builders._streamlit.safe_read",
            lambda file_path: self.mock_safe_read(file_path, "\n".join(mock_content)),
        )

        cmd = BuildCommand(print_warning=False)
        cmd.execute(
            verbose=False,
            build_dir=tmp_path,
            organization_dir=COMPLETE_ORG,
            selected=None,
            build_env_name="dev",
            no_clean=False,
        )

        actual_warning = next((w for w in cmd.warning_list if isinstance(w, StreamlitRequirementsWarning)), None)
        expected_warning = StreamlitRequirementsWarning(expected_result) if len(expected_result) > 0 else None
        assert actual_warning == expected_warning
