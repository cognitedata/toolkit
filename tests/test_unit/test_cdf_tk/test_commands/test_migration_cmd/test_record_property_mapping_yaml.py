import textwrap

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.record_property_mapping import RecordMigrationConfig


def test_empty_mappings_rejected() -> None:
    with pytest.raises(Exception, match="at least one"):
        RecordMigrationConfig.load_yaml("streamExternalId: s\nresourceType: event\nmappings: []\n")


def test_default_mapping_not_in_mappings_rejected() -> None:
    yaml = textwrap.dedent(
        """
        streamExternalId: s
        resourceType: event
        defaultMapping: nonexistent
        mappings:
          - externalId: my-mapping
            containerId: {space: s, externalId: c}
            propertyMapping: {}
        """
    )
    with pytest.raises(Exception, match="nonexistent"):
        RecordMigrationConfig.load_yaml(yaml)


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
    with pytest.raises(Exception, match="Duplicate externalId"):
        RecordMigrationConfig.load_yaml(yaml)
