import textwrap

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.record_property_mapping import load_record_migration_config_yaml
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


def test_load_wrapped_config() -> None:
    yaml = textwrap.dedent(
        """
        streamExternalId: my-stream
        resourceType: event
        mappings:
          - externalId: evt-records
            containerId:
              space: dm
              externalId: MyContainer
            propertyMapping:
              description: description
        """
    )
    config = load_record_migration_config_yaml(yaml)
    assert config.stream_external_id == "my-stream"
    assert config.resource_type == "event"
    assert len(config.mappings) == 1
    assert config.mappings[0].external_id == "evt-records"


def test_root_list_rejected() -> None:
    with pytest.raises(ToolkitValueError, match="Expected a YAML mapping"):
        load_record_migration_config_yaml("[]")


def test_missing_mappings_key_rejected() -> None:
    with pytest.raises(ToolkitValueError, match="Missing required key 'mappings'"):
        load_record_migration_config_yaml("streamExternalId: s\nresourceType: event\n")


def test_empty_mappings_rejected() -> None:
    with pytest.raises(ToolkitValueError, match="at least one"):
        load_record_migration_config_yaml("streamExternalId: s\nresourceType: event\nmappings: []\n")


def test_non_event_resource_type_rejected() -> None:
    with pytest.raises(ToolkitValueError, match="Invalid record migration config"):
        load_record_migration_config_yaml(
            "streamExternalId: s\nresourceType: asset\nmappings:\n"
            "  - externalId: x\n"
            "    containerId: {space: dm, externalId: C}\n"
            "    propertyMapping: {}\n"
        )


def test_duplicate_external_id_rejected() -> None:
    yaml = textwrap.dedent(
        """
        streamExternalId: s
        resourceType: event
        mappings:
          - externalId: dup
            containerId: {space: s, externalId: c}
            propertyMapping: {}
          - externalId: dup
            containerId: {space: s, externalId: c}
            propertyMapping: {}
        """
    )
    with pytest.raises(ToolkitValueError, match="Duplicate externalId"):
        load_record_migration_config_yaml(yaml)
