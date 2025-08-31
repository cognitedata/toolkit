from itertools import groupby
from pathlib import Path

import pytest

from cognite_toolkit._cdf_tk.data_classes import (
    BuildVariables,
    BuiltModule,
    BuiltModuleList,
    BuiltResource,
    BuiltResourceList,
    SourceLocationEager,
)
from cognite_toolkit._cdf_tk.loaders import ResourceTypes


class TestBuiltModuleList:
    @pytest.mark.parametrize(
        "module,resource_dir,kind,selected,expected",
        [
            pytest.param(
                {
                    Path("/module1"): [
                        Path("/module1/transformations/my.Transformation.yaml"),
                        Path("/module1/transformations/my.Schedule.yaml"),
                    ]
                },
                "transformations",
                "Transformation",
                "module1",
                [Path("/module1/transformations/my.Transformation.yaml")],
                id="Select by module name",
            )
        ],
    )
    def test_get_resources_selected(
        self,
        module: dict[Path, list[Path]],
        resource_dir: ResourceTypes,
        kind: str,
        selected: str | Path | None,
        expected: list[Path],
    ) -> None:
        module_list = self._create_built_resource_list(module)
        result = module_list.get_resources(
            id_type=None,
            resource_dir=resource_dir,
            kind=kind,
            selected=selected,
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
