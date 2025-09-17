from dataclasses import dataclass
from itertools import groupby
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.cruds import ResourceTypes
from cognite_toolkit._cdf_tk.data_classes import (
    BuildVariables,
    BuiltModule,
    BuiltModuleList,
    BuiltResource,
    BuiltResourceList,
    SourceLocationEager,
)


@dataclass
class GetResourcesArgs:
    resource_dir: ResourceTypes
    kind: str | None
    selected: str | Path | None


class TestBuiltModuleList:
    # Anchor for absolute paths in tests
    anchor = f"{Path.cwd()}/"

    @pytest.mark.parametrize(
        "module,args,expected",
        [
            pytest.param(
                {
                    Path(f"{anchor}modules/module1"): [
                        Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                        Path(f"{anchor}modules/module1/transformations/my.Schedule.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected="module1",
                ),
                [Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml")],
                id="Select by module name",
            ),
            pytest.param(
                {
                    Path(f"{anchor}modules/module1"): [
                        Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                        Path(f"{anchor}modules/module1/transformations/my.Schedule.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected=Path("modules/module1"),
                ),
                [Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml")],
                id="Select with relative with module in absolute",
            ),
            pytest.param(
                {
                    Path("modules/module1"): [
                        Path("modules/module1/transformations/my.Transformation.yaml"),
                        Path("modules/module1/transformations/my.Schedule.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected=Path(f"{anchor}modules/module1"),
                ),
                [Path("modules/module1/transformations/my.Transformation.yaml")],
                id="Select with absolute with module in relative",
            ),
            pytest.param(
                {
                    Path("modules/module1"): [
                        Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                        Path(f"{anchor}modules/module1/transformations/my.Schedule.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected=Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                ),
                [Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml")],
                id="Select file by absolute path",
            ),
            pytest.param(
                {
                    Path("module1"): [
                        Path("modules/module1/transformations/my.Transformation.yaml"),
                        Path("modules/module1/transformations/my.Schedule.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected=Path("modules/module1/transformations/my.Transformation.yaml"),
                ),
                [Path("modules/module1/transformations/my.Transformation.yaml")],
                id="Select file by relative path",
            ),
            pytest.param(
                {
                    Path("modules/module1"): [
                        Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                        Path(f"{anchor}modules/module1/transformations/my.Schedule.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected=Path(f"{anchor}modules/module1/transformations"),
                ),
                [Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml")],
                id="Select resource dir by absolute path",
            ),
            pytest.param(
                {
                    Path("module1"): [
                        Path("modules/module1/transformations/my.Transformation.yaml"),
                        Path("modules/module1/transformations/my.Schedule.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected=Path("modules/module1/transformations/my.Transformation.yaml"),
                ),
                [Path("modules/module1/transformations/my.Transformation.yaml")],
                id="Select resource dir by relative path",
            ),
            pytest.param(
                {
                    Path(f"{anchor}module1"): [
                        Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                        Path(f"{anchor}modules/module1/transformations/my.Schedule.yaml"),
                    ],
                    Path(f"{anchor}module2"): [
                        Path(f"{anchor}module2/transformations/other.Transformation.yaml"),
                    ],
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected=None,
                ),
                [
                    Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                    Path(f"{anchor}module2/transformations/other.Transformation.yaml"),
                ],
                id="Select all by kind",
            ),
            pytest.param(
                {
                    Path(f"{anchor}module1"): [
                        Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="NonExistentKind",
                    selected=None,
                ),
                [],
                id="Select with non-existent kind",
            ),
            pytest.param(
                {
                    Path("modules/module1"): [
                        Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected=Path("/modules/module1/transformations/doesnotexist.yaml"),
                ),
                [],
                id="Select with non-existent path",
            ),
            pytest.param(
                {
                    Path("modules/module1"): [
                        Path(f"{anchor}modules/module1/transformations/my.Transformation.yaml"),
                    ]
                },
                GetResourcesArgs(
                    resource_dir="transformations",
                    kind="Transformation",
                    selected="notamodule",
                ),
                [],
                id="Select with non-existent module name",
            ),
        ],
    )
    def test_get_resources_selected(
        self,
        module: dict[Path, list[Path]],
        args: GetResourcesArgs,
        expected: list[Path],
    ) -> None:
        module_list = self._create_built_resource_list(module)
        result = module_list.get_resources(
            id_type=None,
            resource_dir=args.resource_dir,
            kind=args.kind,
            selected=args.selected,
        )
        actual = [item.source.path for item in result]
        assert actual == expected

    @staticmethod
    def _create_built_resource_list(module: dict[Path, list[Path]]) -> BuiltModuleList:
        modules: list[BuiltModule] = [
            BuiltModule(
                name=module_path.name,
                location=SourceLocationEager(module_path, "hash1234"),
                build_variables=BuildVariables([]),
                resources={
                    resource_dir: BuiltResourceList(
                        [
                            BuiltResource(
                                identifier=f"resource_{i}",
                                source=SourceLocationEager(resource_path, "hash5678"),
                                kind=resource_path.stem.split(".")[-1],
                                destination=None,
                                extra_sources=None,
                            )
                            for i, resource_path in enumerate(resource_paths)
                        ]
                    )
                    for resource_dir, resource_paths in groupby(
                        sorted(resource_paths, key=lambda p: p.parent.name), key=lambda p: p.parent.name
                    )
                },
                warning_count=0,
                status="success",
                iteration=1,
            )
            for module_path, resource_paths in module.items()
        ]
        module_list = BuiltModuleList(modules)
        return module_list
