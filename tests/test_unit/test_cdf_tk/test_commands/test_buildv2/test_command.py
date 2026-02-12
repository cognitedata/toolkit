from pathlib import Path

from cognite_toolkit._cdf_tk.commands import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildParameters
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import Recommendation
from cognite_toolkit._cdf_tk.cruds import SpaceCRUD


class TestBuildCommand:
    def test_end_to_end(self, tmp_path: Path) -> None:
        cmd = BuildV2Command()

        # Set up a simple organization with modules folder.
        org = tmp_path / "org"
        resource_file = org / "modules" / "my_module" / SpaceCRUD.folder_name / f"my_space.{SpaceCRUD.kind}.yaml"
        resource_file.parent.mkdir(parents=True)
        space_yaml = """space: my_space
name: My Space
"""
        resource_file.write_text(space_yaml)
        build_dir = tmp_path / "build"
        parameters = BuildParameters(organization_dir=org, build_dir=build_dir)

        folder = cmd.build(parameters)

        assert folder

        built_space = list(build_dir.rglob(f"*.{SpaceCRUD.kind}.yaml"))
        assert len(built_space) == 1
        assert built_space[0].read_text() == space_yaml

        assert SpaceCRUD.folder_name in folder.resource_by_type
        assert str(folder.resource_by_type[SpaceCRUD.folder_name][SpaceCRUD.kind][0]) == str(built_space[0])
        assert len(folder.insights) == 1
        assert isinstance(folder.insights[0], Recommendation)

    def test_end_to_end_failed_build(self, tmp_path: Path) -> None:
        cmd = BuildV2Command()

        # Set up a simple organization with modules folder.
        org = tmp_path / "org"
        resource_file = org / "modules" / "my_module" / SpaceCRUD.folder_name / f"my_space.{SpaceCRUD.kind}.yaml"
        resource_file.parent.mkdir(parents=True)
        space_yaml = """space: my#space
name: My Space
"""
        resource_file.write_text(space_yaml)
        build_dir = tmp_path / "build"
        parameters = BuildParameters(organization_dir=org, build_dir=build_dir)

        folder = cmd.build(parameters)

        assert folder.resource_by_type == {}
        assert len(folder.insights) == 1
