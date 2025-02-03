from pathlib import Path

from cognite_toolkit._cdf_tk.commands.build import BuildCommand
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

        warns = [w for w in cmd.warning_list if hasattr(w, "filepath") and "hosted_extractors" in w.filepath.parts]

        assert len(warns) == 0, warns
