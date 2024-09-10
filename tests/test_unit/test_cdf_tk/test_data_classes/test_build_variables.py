from __future__ import annotations

import yaml

from cognite_toolkit._cdf_tk.data_classes import BuildVariables


class TestBuildVariables:
    def test_load(self) -> None:
        assert True

    def test_replace_preserve_data_type(self) -> None:
        source_yaml = """text: {{ my_text }}
bool: {{ my_bool }}
integer: {{ my_integer }}
float: {{ my_float }}
digit_string: {{ my_digit_string }}
quoted_string: "{{ my_quoted_string }}"
list: {{ my_list }}
null_value: {{ my_null_value }}
single_quoted_string: '{{ my_single_quoted_string }}'
composite: 'some_prefix_{{ my_composite }}'
prefix_text: {{ my_prefix_text }}
suffix_text: {{ my_suffix_text }}
"""
        variables = BuildVariables.load_raw(
            {
                "my_text": "some text",
                "my_bool": True,
                "my_integer": 123,
                "my_float": 123.456,
                "my_digit_string": "123",
                "my_quoted_string": "456",
                "my_list": ["one", "two", "three"],
                "my_null_value": None,
                "my_single_quoted_string": "789",
                "my_composite": "the suffix",
                "my_prefix_text": "prefix:",
                "my_suffix_text": ":suffix",
            },
            available_modules={tuple()},
            selected_modules={tuple()},
        )

        result = variables.replace(source_yaml)

        loaded = yaml.safe_load(result)
        assert loaded == {
            "text": "some text",
            "bool": True,
            "integer": 123,
            "float": 123.456,
            "digit_string": "123",
            "quoted_string": "456",
            "list": ["one", "two", "three"],
            "null_value": None,
            "single_quoted_string": "789",
            "composite": "some_prefix_the suffix",
            "prefix_text": "prefix:",
            "suffix_text": ":suffix",
        }

    def test_replace_not_preserve_type(self) -> None:
        source_yaml = """dataset_id('{{dataset_external_id}}')"""
        variables = BuildVariables.load_raw(
            {
                "dataset_external_id": "ds_external_id",
            },
            available_modules={tuple()},
            selected_modules={tuple()},
        )

        result = variables.replace(source_yaml, file_suffix=".sql")

        assert result == "dataset_id('ds_external_id')"
