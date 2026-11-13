from collections.abc import Iterable
from pathlib import Path
from typing import get_args

import pytest
import yaml

from cognite_toolkit._cdf_tk.data_classes._insights import BuildWarning, Recommendation
from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags
from cognite_toolkit._cdf_tk.resource_ios import ViewIO
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses
from cognite_toolkit._cdf_tk.yaml_classes.view_field_definitions import ViewProperty, ViewPropertyType
from cognite_toolkit._cdf_tk.yaml_classes.views import ViewYAML
from tests.test_unit.utils import find_resources


def view_property_type_cases() -> Iterable:
    view_property_types = [
        {
            "CreateViewProperty": {
                "space": "my_space",
                "externalId": "my_view",
                "version": "1",
                "name": "My View",
                "description": "A test view with create view property",
                "properties": {
                    "textField": {
                        "name": "Text Field",
                        "description": "A text field from container",
                        "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                        "containerPropertyIdentifier": "textField",
                    }
                },
            }
        },
        {
            "SingleEdgeConnection": {
                "space": "my_space",
                "externalId": "edge_view",
                "version": "1",
                "properties": {
                    "connectedTo": {
                        "connectionType": "single_edge_connection",
                        "source": {"type": "view", "space": "my_space", "externalId": "source_view", "version": "1"},
                        "type": {"space": "my_space", "externalId": "edge_type"},
                        "direction": "outwards",
                    }
                },
            }
        },
        {
            "MultiEdgeConnection": {
                "space": "my_space",
                "externalId": "multi_edge_view",
                "version": "1",
                "properties": {
                    "manyConnections": {
                        "connectionType": "multi_edge_connection",
                        "source": {"type": "view", "space": "my_space", "externalId": "source_view", "version": "1"},
                        "type": {"space": "my_space", "externalId": "edge_type"},
                        "direction": "inwards",
                    }
                },
            }
        },
        {
            "SingleReverseDirectRelation": {
                "space": "my_space",
                "externalId": "reverse_view",
                "version": "1",
                "properties": {
                    "reverseConnection": {
                        "connectionType": "single_reverse_direct_relation",
                        "source": {"type": "view", "space": "my_space", "externalId": "source_view", "version": "1"},
                        "through": {
                            "source": {"type": "container", "space": "my_space", "externalId": "relation_container"},
                            "identifier": "connectedTo",
                        },
                    }
                },
            }
        },
        {
            "MultiReverseDirectRelation": {
                "space": "my_space",
                "externalId": "multi_reverse_view",
                "version": "1",
                "properties": {
                    "multiReverseConnection": {
                        "connectionType": "multi_reverse_direct_relation",
                        "source": {"type": "view", "space": "my_space", "externalId": "source_view", "version": "1"},
                        "through": {
                            "source": {
                                "type": "view",
                                "space": "my_space",
                                "externalId": "relation_view",
                                "version": "1",
                            },
                            "identifier": "connectedTo",
                        },
                    }
                },
            }
        },
    ]
    yield from (pytest.param(next(iter(pt.values())), id=next(iter(pt.keys()))) for pt in view_property_types)


def view_with_implements_cases() -> Iterable:
    view_implements = [
        {
            "ImplementsView": {
                "space": "my_space",
                "externalId": "implements_view",
                "version": "1",
                "implements": [{"type": "view", "space": "my_space", "externalId": "base_view", "version": "1"}],
                "properties": {
                    "name": {
                        "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                        "containerPropertyIdentifier": "name",
                    }
                },
            }
        },
        {
            "ImplementsMultipleViews": {
                "space": "my_space",
                "externalId": "multi_implements_view",
                "version": "1",
                "implements": [
                    {"type": "view", "space": "my_space", "externalId": "base_view1", "version": "1"},
                    {"type": "view", "space": "my_space", "externalId": "base_view2", "version": "1"},
                ],
                "properties": {
                    "identifier": {
                        "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                        "containerPropertyIdentifier": "identifier",
                    }
                },
            }
        },
    ]
    yield from (pytest.param(next(iter(i.values())), id=next(iter(i.keys()))) for i in view_implements)


def view_with_filters_cases() -> Iterable:
    view_filters = [
        {
            "SimpleFilter": {
                "space": "my_space",
                "externalId": "filtered_view",
                "version": "1",
                "filter": {"equals": {"property": ["status"], "value": "active"}},
                "properties": {
                    "name": {
                        "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                        "containerPropertyIdentifier": "name",
                    }
                },
            }
        },
    ]
    yield from (pytest.param(next(iter(f.values())), id=next(iter(f.keys()))) for f in view_filters)


class TestViewYAML:
    @pytest.mark.parametrize("data", list(find_resources("View")))
    def test_load_valid_view_file(self, data: dict[str, object]) -> None:
        loaded = ViewYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", view_property_type_cases())
    def test_load_valid_view_property_types(self, data: dict[str, object]) -> None:
        loaded = ViewYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", view_with_implements_cases())
    def test_load_valid_view_implements(self, data: dict[str, object]) -> None:
        loaded = ViewYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize("data", view_with_filters_cases())
    def test_load_valid_view_filters(self, data: dict[str, object]) -> None:
        loaded = ViewYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    def test_invalid_external_id(self) -> None:
        invalid_data = {
            "space": "my_space",
            "externalId": "Query",  # Reserved name
            "version": "1",
            "properties": {
                "field": {
                    "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                    "containerPropertyIdentifier": "field",
                }
            },
        }

        with pytest.raises(ValueError, match="is a reserved view External ID"):
            ViewYAML.model_validate(invalid_data)

    def test_invalid_property_identifier(self) -> None:
        invalid_data = {
            "space": "my_space",
            "externalId": "valid_view",
            "version": "1",
            "properties": {
                "invalid@": {  # Invalid character
                    "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                    "containerPropertyIdentifier": "field",
                }
            },
        }

        with pytest.raises(ValueError, match="does not match the required pattern"):
            ViewYAML.model_validate(invalid_data)

    def test_forbidden_property_identifier(self) -> None:
        invalid_data = {
            "space": "my_space",
            "externalId": "valid_view",
            "version": "1",
            "properties": {
                "edge_id": {  # Reserved property name
                    "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                    "containerPropertyIdentifier": "field",
                }
            },
        }

        with pytest.raises(ValueError, match="is a reserved property identifier"):
            ViewYAML.model_validate(invalid_data)

    def test_space_format_validation(self) -> None:
        invalid_data = {
            "space": "invalid space",  # Contains a space
            "externalId": "valid_view",
            "version": "1",
            "properties": {
                "field": {
                    "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                    "containerPropertyIdentifier": "field",
                }
            },
        }

        with pytest.raises(ValueError):
            ViewYAML.model_validate(invalid_data)

    def test_invalid_version_format(self) -> None:
        invalid_data = {
            "space": "my_space",
            "externalId": "valid_view",
            "version": ".0.1",  # Invalid version format
            "properties": {
                "field": {
                    "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                    "containerPropertyIdentifier": "field",
                }
            },
        }

        with pytest.raises(ValueError):
            ViewYAML.model_validate(invalid_data)

    def test_load_valid_record_view_with_stream_id(self) -> None:
        data = {
            "space": "my_space",
            "externalId": "my_record_view",
            "version": "1",
            "streamId": ["my_stream"],
            "properties": {
                "name": {
                    "container": {"type": "container", "space": "my_space", "externalId": "my_container"},
                    "containerPropertyIdentifier": "name",
                }
            },
        }
        loaded = ViewYAML.model_validate(data)
        assert loaded.model_dump(exclude_unset=True, by_alias=True) == data

    @pytest.mark.parametrize(
        "filter_yaml, expected",
        [
            pytest.param(
                "{nested: {scope: [my_space, my_view/1, related], filter: {equals: {property: [node, space], "
                "value: my_space}}}}",
                [(Recommendation, "DEGRADED-PERFORMANCE", 4)],
                id="nested",
            ),
            pytest.param(
                "{not: {nested: {scope: [my_space, my_view/1, related], filter: {exists: {property: [node, space]}}}}}",
                [(Recommendation, "DEGRADED-PERFORMANCE", 4)],
                id="nested-within-not",
            ),
            pytest.param(
                "{and: [{hasData: [{type: view, space: my_space, externalId: other_view, version: '1'}]}]}",
                [(BuildWarning, "ANTI-PATTERN", 4)],
                id="has-data-on-view",
            ),
            pytest.param(
                "{hasData: [{type: container, space: my_space, externalId: my_container}]}", [], id="has-data-container"
            ),
        ],
    )
    def test_filter_insights(
        self, filter_yaml: str, expected: list[tuple[type, str, int]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(FeatureFlag, "is_enabled", lambda flag: flag is Flags.ALPHA_RULES)
        source = tmp_path / "my.View.yaml"
        source.write_text(f"space: my_space\nexternalId: my_view\nversion: '1'\nfilter: {filter_yaml}\n")
        view = ViewYAML.model_validate(yaml.safe_load(source.read_text()))

        insights = view.extra_insights(source, ViewIO.as_resource_type())
        assert [(type(insight), insight.code, insight.line) for insight in insights] == expected

    def test_all_view_properties_in_union(self) -> None:
        """Test that all view property types are included in the union."""
        expected_subclasses = set(get_concrete_subclasses(ViewProperty))
        subclasses = set(get_args(ViewPropertyType.__args__[0]))

        assert subclasses == expected_subclasses, f"Expected subclasses {expected_subclasses}, but got {subclasses}"
