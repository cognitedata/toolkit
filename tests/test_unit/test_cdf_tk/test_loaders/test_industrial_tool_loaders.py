#  default_packages = {
#             "pyodide-http": "0.2.1",
#             "cognite-sdk": CogniteSDKVersion.__version__,
#         }


from pathlib import Path

import cognite_toolkit._cdf_tk.utils.file
from cognite_toolkit._cdf_tk.commands.build import BuildCommand
from cognite_toolkit._cdf_tk.tk_warnings.other import StreamlitRequirementsWarning
from cognite_toolkit._cdf_tk.utils.file import safe_read
from tests.data import COMPLETE_ORG


class TestStreamlitLoader:
    @staticmethod
    def mock_safe_read(file_path, mock_content):
        if file_path.name == "requirements.txt":
            return mock_content  # Your mocked content
        else:
            # If it's not requirements.txt, call the original safe_read
            return safe_read(file_path)

    def test_requirements_file_missing_packages_warnings(self, monkeypatch, tmp_path: Path) -> None:
        mock_content = "numpy"
        monkeypatch.setattr(
            cognite_toolkit._cdf_tk.utils.file,
            "safe_read",
            lambda file_path: self.mock_safe_read(file_path, mock_content),
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

        warns = [w for w in cmd.warning_list if isinstance(w, StreamlitRequirementsWarning)]

        assert len(warns) == 1
        assert "pyodide-http" in warns[0].dependencies
        assert "cognite-sdk" in warns[0].dependencies
