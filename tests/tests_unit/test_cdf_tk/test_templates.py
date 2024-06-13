from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
import yaml

from cognite_toolkit._cdf_tk.commands.build import BuildCommand, _BuildState, _Helpers
from cognite_toolkit._cdf_tk.data_classes import (
    BuildConfigYAML,
    ConfigEntry,
    Environment,
    InitConfigYAML,
    SystemYAML,
)
from cognite_toolkit._cdf_tk.loaders import LOADER_BY_FOLDER_NAME
from cognite_toolkit._cdf_tk.utils import (
    YAMLComment,
    flatten_dict,
    iterate_modules,
    module_from_path,
)
from tests.tests_unit.data import PYTEST_PROJECT
from tests.tests_unit.test_cdf_tk.constants import BUILD_DIR


def dict_keys(d: dict[str, Any]) -> set[str]:
    keys = set()
    for k, v in d.items():
        keys.add(k)
        if isinstance(v, dict):
            keys.update(dict_keys(v))
    return keys


@pytest.fixture(scope="session")
def config_yaml() -> str:
    return (PYTEST_PROJECT / "config.dev.yaml").read_text()


@pytest.fixture(scope="session")
def dummy_environment() -> Environment:
    return Environment(
        name="dev",
        project="my_project",
        build_type="dev",
        selected=["none"],
    )


class TestConfigYAML:
    def test_producing_correct_keys(self, config_yaml: str, dummy_environment: Environment) -> None:
        expected_keys = set(flatten_dict(yaml.safe_load(config_yaml)))
        # Custom keys are not loaded from the module folder.
        # This custom key is added o the dev.config.yaml for other tests.
        expected_keys.remove(("variables", "custom_modules", "my_example_module", "transformation_is_paused"))
        # Skip all environment variables
        expected_keys = {k for k in expected_keys if not k[0] == "environment"}

        config = InitConfigYAML(dummy_environment).load_defaults(PYTEST_PROJECT)

        actual_keys = set(config.keys())
        missing = expected_keys - actual_keys
        assert not missing, f"Missing keys: {missing}"
        extra = actual_keys - expected_keys
        assert not extra, f"Extra keys: {extra}"

    def test_extract_extract_config_yaml_comments(self, config_yaml: str) -> None:
        expected_comments = {
            ("variables", "cognite_modules", "a_module", "readonly_source_id"): YAMLComment(
                above=["This is a comment in the middle of the file"], after=[]
            ),
            ("variables", "cognite_modules", "another_module", "default_location"): YAMLComment(
                above=["This is a comment at the beginning of the module."]
            ),
            ("variables", "cognite_modules", "another_module", "source_asset"): YAMLComment(
                after=["This is an extra comment added to the config only 'lore ipsum'"]
            ),
            ("variables", "cognite_modules", "another_module", "source_files"): YAMLComment(
                after=["This is a comment after a variable"]
            ),
        }

        actual_comments = InitConfigYAML._extract_comments(config_yaml)

        assert actual_comments == expected_comments

    @pytest.mark.parametrize(
        "raw_file, key_prefix, expected_comments",
        [
            pytest.param(
                """---
# This is a module comment
variable: value # After variable comment
# Before variable comment
variable2: value2
variable3: 'value with #in it'
variable4: "value with #in it" # But a comment after
""",
                tuple("super_module.module_a".split(".")),
                {
                    ("super_module", "module_a", "variable"): YAMLComment(
                        after=["After variable comment"], above=["This is a module comment"]
                    ),
                    ("super_module", "module_a", "variable2"): YAMLComment(above=["Before variable comment"]),
                    ("super_module", "module_a", "variable4"): YAMLComment(after=["But a comment after"]),
                },
                id="module comments",
            )
        ],
    )
    def test_extract_default_config_comments(
        self, raw_file: str, key_prefix: tuple[str, ...], expected_comments: dict[str, Any]
    ):
        actual_comments = InitConfigYAML._extract_comments(raw_file, key_prefix)
        assert actual_comments == expected_comments

    def test_persist_variable_with_comment(self, config_yaml: str) -> None:
        custom_comment = "This is an extra comment added to the config only 'lore ipsum'"

        config = InitConfigYAML.load_existing(config_yaml).load_defaults(PYTEST_PROJECT)

        dumped = config.dump_yaml_with_comments()
        loaded = yaml.safe_load(dumped)
        assert loaded["variables"]["cognite_modules"]["another_module"]["source_asset"] == "my_new_workmate"
        assert custom_comment in dumped

    def test_added_and_removed_variables(self, config_yaml: str) -> None:
        existing_config_yaml = yaml.safe_load(config_yaml)
        # Added = Exists in the BUILD_CONFIG directory default.config.yaml files but not in config.yaml
        existing_config_yaml["variables"]["cognite_modules"]["another_module"].pop("source_asset")
        # Removed = Exists in config.yaml but not in the BUILD_CONFIG directory default.config.yaml files
        existing_config_yaml["variables"]["cognite_modules"]["another_module"]["removed_variable"] = "old_value"

        config = InitConfigYAML.load_existing(yaml.safe_dump(existing_config_yaml)).load_defaults(PYTEST_PROJECT)

        removed = [v for v in config.values() if v.default_value is None]
        # There is already a custom variable in the config.yaml file
        assert len(removed) == 2
        assert ("variables", "cognite_modules", "another_module", "removed_variable") in [v.key_path for v in removed]

        added = [v for v in config.values() if v.current_value is None]
        assert len(added) == 1
        assert added[0].key_path == ("variables", "cognite_modules", "another_module", "source_asset")

    def test_load_variables(self, dummy_environment: Environment) -> None:
        expected = {
            ("variables", "cognite_modules", "a_module", "readonly_source_id"),
            # default_location is used in two modules and is moved to the top level
            ("variables", "cognite_modules", "default_location"),
            ("variables", "cognite_modules", "another_module", "source_files"),
            ("variables", "cognite_modules", "another_module", "model_space"),
            ("variables", "cognite_modules", "parent_module", "child_module", "source_asset"),
        }

        config = InitConfigYAML(dummy_environment).load_variables(PYTEST_PROJECT, propagate_reused_variables=True)

        missing = expected - set(config.keys())
        extra = set(config.keys()) - expected
        assert not missing, f"Missing keys: {missing}. Got extra {extra}"
        assert not extra, f"Extra keys: {extra}"

    def test_load_parent_variables(self, dummy_environment: Environment) -> None:
        config = InitConfigYAML(
            dummy_environment,
            {
                ("variables", "cognite_modules", "infield", "shared_variable"): ConfigEntry(
                    key_path=("variables", "cognite_modules", "infield", "shared_variable"),
                    default_value="shared_value",
                )
            },
        )

        config._load_variables({"shared_variable": {("cognite_modules", "infield", "cdf_infield_common")}})

        assert ("variables", "cognite_modules", "infield", "shared_variable") in config.keys()
        assert ("variables", "cognite_modules", "infield", "cdf_infield_common", "shared_variable") not in config.keys()

    def test_finds_selected_defaults(
        self,
    ) -> None:
        environment = Environment(
            name="dev",
            project="my_project",
            build_type="dev",
            selected=["cognite_modules/a_module"],
        )

        config_all = InitConfigYAML(environment).load_defaults(PYTEST_PROJECT)
        config_selected = InitConfigYAML(environment).load_selected_defaults(PYTEST_PROJECT)

        assert len(config_all) > len(config_selected)
        assert ("variables", "cognite_modules", "a_module", "readonly_source_id") in config_all.keys()
        assert ("variables", "cognite_modules", "a_module", "readonly_source_id") in config_selected.keys()

        assert ("variables", "cognite_modules", "parent_module", "child_module", "child_variable") in config_all.keys()
        assert (
            "variables",
            "cognite_modules",
            "parent_module",
            "child_module",
            "child_variable",
        ) not in config_selected.keys()


@pytest.mark.parametrize(
    "input_, expected",
    [
        pytest.param({"a": {"b": 1, "c": 2}}, {("a", "b"): 1, ("a", "c"): 2}, id="Simple"),
        pytest.param({"a": {"b": {"c": 1}}}, {("a", "b", "c"): 1}, id="Nested"),
    ],
)
def test_flatten_dict(input_: dict[str, Any], expected: dict[str, Any]) -> None:
    actual = flatten_dict(input_)

    assert actual == expected


@pytest.fixture()
def my_config():
    return {
        "top_variable": "my_top_variable",
        "module_a": {
            "readwrite_source_id": "my_readwrite_source_id",
            "readonly_source_id": "my_readonly_source_id",
        },
        "parent": {"child": {"child_variable": "my_child_variable"}},
    }


def test_split_config(my_config: dict[str, Any]) -> None:
    expected = {
        "": {"top_variable": "my_top_variable"},
        "module_a": {
            "readwrite_source_id": "my_readwrite_source_id",
            "readonly_source_id": "my_readonly_source_id",
        },
        "parent.child": {"child_variable": "my_child_variable"},
    }
    actual = _Helpers.to_variables_by_module_path(my_config)

    assert actual == expected


def test_create_local_config(my_config: dict[str, Any]):
    configs = _Helpers.to_variables_by_module_path(my_config)

    local_config = _Helpers.create_local_config(configs, Path("parent/child/auth/"))

    assert dict(local_config.items()) == {"top_variable": "my_top_variable", "child_variable": "my_child_variable"}


def valid_yaml_semantics_test_cases() -> Iterable[pytest.ParameterSet]:
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
    def test_valid_yaml(self, raw_yaml: str, source_path: Path, dummy_environment: Environment):
        state = _BuildState.create(BuildConfigYAML(dummy_environment, filepath=Path("dummy"), variables={}))
        cmd = BuildCommand(print_warning=False)
        # Only used in error messages
        destination = Path("build/raw/raw.yaml")
        yaml_warnings = cmd.validate(raw_yaml, source_path, destination, state, False)
        assert not yaml_warnings


class TestIterateModules:
    def test_modules_project_for_tests(self):
        expected_modules = {
            PYTEST_PROJECT / "cognite_modules" / "a_module",
            PYTEST_PROJECT / "cognite_modules" / "another_module",
            PYTEST_PROJECT / "cognite_modules" / "parent_module" / "child_module",
        }

        actual_modules = {module for module, _ in iterate_modules(PYTEST_PROJECT)}

        assert actual_modules == expected_modules


class TestModuleFromPath:
    @pytest.mark.parametrize(
        "path, expected",
        [
            pytest.param(Path("cognite_modules/a_module/data_models/my_model.datamodel.yaml"), "a_module"),
            pytest.param(Path("cognite_modules/another_module/data_models/views/my_view.view.yaml"), "another_module"),
            pytest.param(
                Path("cognite_modules/parent_module/child_module/data_models/containers/my_container.container.yaml"),
                "child_module",
            ),
            pytest.param(
                Path("cognite_modules/parent_module/child_module/data_models/auth/my_group.group.yaml"), "child_module"
            ),
            pytest.param(Path("custom_modules/child_module/functions/functions.yaml"), "child_module"),
            pytest.param(Path("custom_modules/parent_module/child_module/functions/functions.yaml"), "child_module"),
        ],
    )
    def test_module_from_path(self, path: Path, expected: str):
        assert module_from_path(path) == expected


class TestBuildConfigYAML:
    def test_build_config_create_valid_build_folder(self, config_yaml: str) -> None:
        build_env_name = "dev"
        system_config = SystemYAML.load_from_directory(PYTEST_PROJECT, build_env_name)
        config = BuildConfigYAML.load_from_directory(PYTEST_PROJECT, build_env_name)
        available_modules = {module.name for module, _ in iterate_modules(PYTEST_PROJECT)}
        config.environment.selected = list(available_modules)

        BuildCommand().build_config(
            BUILD_DIR, PYTEST_PROJECT, config=config, system_config=system_config, clean=True, verbose=False
        )

        # The resulting build folder should only have subfolders that are matching the folder name
        # used by the loaders.
        invalid_resource_folders = [
            dir_.name for dir_ in BUILD_DIR.iterdir() if dir_.is_dir() and dir_.name not in LOADER_BY_FOLDER_NAME
        ]
        assert not invalid_resource_folders, f"Invalid resource folders after build: {invalid_resource_folders}"

    @pytest.mark.parametrize(
        "modules, expected_available_modules",
        [
            pytest.param({"another_module": {}}, ["another_module"], id="Single module"),
            pytest.param(
                {
                    "cognite_modules": {
                        "top_variable": "my_top_variable",
                        "a_module": {
                            "source_id": "123-456-789",
                        },
                        "parent_module": {
                            "parent_variable": "my_parent_variable",
                            "child_module": {
                                "dataset_external_id": "ds_my_dataset",
                            },
                        },
                        "module_without_variables": {},
                    }
                },
                ["a_module", "child_module", "module_without_variables"],
                id="Multiple nested modules",
            ),
        ],
    )
    def test_available_modules(
        self, modules: dict[str, Any], expected_available_modules: list[str], dummy_environment: Environment
    ) -> None:
        config = BuildConfigYAML(dummy_environment, filepath=Path("dummy"), variables=modules)

        assert sorted(config.available_modules) == sorted(expected_available_modules)
