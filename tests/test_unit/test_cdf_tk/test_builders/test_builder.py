from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pytest
from _pytest.mark import ParameterSet
from cognite.client.data_classes.data_modeling import DataModelId

from cognite_toolkit._cdf_tk.builders import Builder
from cognite_toolkit._cdf_tk.exceptions import AmbiguousResourceFileError
from cognite_toolkit._cdf_tk.loaders import DataModelLoader


def valid_yaml_semantics_test_cases() -> Iterable[ParameterSet]:
    yield pytest.param(
        """
- dbName: src:005:test:rawdb:state
- dbName: src:002:weather:rawdb:state
- dbName: uc:001:demand:rawdb:state
- dbName: in:all:rawdb:state
- dbName: src:001:sap:rawdb
""",
        Path("build/raw/raw.yaml"),
        id="Multiple Raw Databases",
    )

    yield pytest.param(
        """
dbName: src:005:test:rawdb:state
""",
        Path("build/raw/raw.yaml"),
        id="Single Raw Database",
    )

    yield pytest.param(
        """
dbName: src:005:test:rawdb:state
tableName: myTable
""",
        Path("build/raw/raw.yaml"),
        id="Single Raw Database with table",
    )

    yield pytest.param(
        """
- dbName: src:005:test:rawdb:state
  tableName: myTable
- dbName: src:002:weather:rawdb:state
  tableName: myOtherTable
""",
        Path("build/raw/raw.yaml"),
        id="Multiple Raw Databases with table",
    )


class TestCheckYamlSemantics:
    @pytest.mark.parametrize("raw_yaml, source_path", list(valid_yaml_semantics_test_cases()))
    def test_valid_yaml(self, raw_yaml: str, source_path: Path) -> None:
        builder = Builder(Path(), {}, silent=True, verbose=False, resource_folder="raw")
        # Only used in error messages
        destination = Path("build/raw/raw.yaml")
        yaml_warnings, *_ = builder.validate(raw_yaml, source_path, destination)
        assert not yaml_warnings

    def test_build_valid_read_int_version(self) -> None:
        builder = Builder(Path(), {}, silent=True, verbose=False, resource_folder="transformations")
        destination = Path("build/transformation/transformations.Transformation.yaml")
        source_path = Path("my_module/transformations/transformations.Transformation.yaml")
        raw_yaml = """destination:
  dataModel:
    destinationType: CogniteFile
    externalId: MyModel
    space: my_space
    version: 1_0_0
  instanceSpace: my_space
  type: instances
externalId: some_external_id
    """
        _, identifier_pairs = builder.validate(raw_yaml, source_path, destination)
        assert len(identifier_pairs) == 1
        required_data_model = next(
            (required_id for loader, required_id in builder.dependencies_by_required if loader is DataModelLoader),
            None,
        )
        assert required_data_model is not None
        assert required_data_model == DataModelId("my_space", "MyModel", "1_0_0")


class TestBuilder:
    def test_get_loader_raises_ambiguous_error(self):
        builder = Builder(Path(), {}, silent=True, verbose=False, resource_folder="transformations")

        with pytest.raises(AmbiguousResourceFileError) as e:
            builder._get_loader(
                "transformations",
                destination=Path("transformation") / "notification.yaml",
                source_path=Path("my_module") / "transformations" / "notification.yaml",
            )
        assert "Ambiguous resource file" in str(e.value)
