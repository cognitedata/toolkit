from pathlib import Path

from cognite_toolkit._cdf_tk.commands.build_cmd import BuildCommand
from cognite_toolkit._cdf_tk.cruds import HostedExtractorSourceCRUD
from cognite_toolkit._cdf_tk.tk_warnings import MissingRequiredParameterWarning
from tests.data import COMPLETE_ORG


class TestHostedExtractorDestinationLoader:
    def test_hosted_extractor_destination_spec(self, tmp_path: Path) -> None:
        cmd = BuildCommand(print_warning=False)
        cmd.execute(
            verbose=False,
            build_dir=tmp_path,
            organization_dir=COMPLETE_ORG,
            selected=None,
            build_env_name="dev",
            no_clean=False,
        )

        warns = [
            w
            for w in cmd.warning_list
            if isinstance(w, MissingRequiredParameterWarning)
            and HostedExtractorSourceCRUD.folder_name in w.filepath.parts
        ]

        assert len(warns) == 0, warns
