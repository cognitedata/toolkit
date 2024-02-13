from typing import Any

import pytest

from cognite_toolkit.cdf_tk.pull import ResourceYAMLDifference


def load_update_diffs_use_cases():
    build_file = """externalId: tr_pump_asset_hierarchy-load-collections_pump
name: pump_asset_hierarchy-load-collections_pump
destination:
  type: asset_hierarchy
dataSetExternalId: src:lift_pump_stations
ignoreNullFields: false
# Specify credentials separately like this:
# You can also use different credentials for the running transformations than the ones you use to deploy
authentication:
  clientId: ${IDP_CLIENT_ID}
  clientSecret: ${IDP_CLIENT_SECRET}
  tokenUri: ${IDP_TOKEN_URL}
  # Optional: If idP requires providing the scopes
  cdfProjectName: ${CDF_PROJECT}
  scopes:
  - ${IDP_SCOPES}
  # Optional: If idP requires providing the audience
  audience: ${IDP_AUDIENCE}
"""
    source_file = """externalId: tr_pump_asset_hierarchy-load-collections_pump
name: pump_asset_hierarchy-load-collections_pump
destination:
  type: asset_hierarchy
dataSetExternalId: {{data_set}}
ignoreNullFields: false
# Specify credentials separately like this:
# You can also use different credentials for the running transformations than the ones you use to deploy
authentication:
  clientId: {{cicd_clientId}}
  clientSecret: {{cicd_clientSecret}}
  tokenUri: {{cicd_tokenUri}}
  # Optional: If idP requires providing the scopes
  cdfProjectName: {{cdfProjectName}}
  scopes: {{cicd_scopes}}
  # Optional: If idP requires providing the audience
  audience: {{cicd_audience}}
"""
    cdf_resource = {
        "conflictMode": "upsert",
        "destination": {"type": "asset_hierarchy"},
        "externalId": "tr_pump_asset_hierarchy-load-collections_pump",
        "ignoreNullFields": False,
        "isPublic": True,
        "name": "pump_asset_hierarchy-load-collections_pump",
    }
    expected = {
        "added": {"isPublic": True, "conflictMode": "upsert"},
        "changed": {},
        "cannot_change": {},
    }

    yield pytest.param(build_file, source_file, cdf_resource, expected, id="Transformation with no differences")


class TestResourceYAML:
    @pytest.mark.parametrize(
        "build_file, source_file, cdf_resource, expected",
        list(load_update_diffs_use_cases()),
    )
    def test_load_update_diffs(
        self, build_file: str, source_file: str, cdf_resource: dict[str, Any], expected: dict[str, dict[str, Any]]
    ) -> None:
        resource_yaml = ResourceYAMLDifference.load(build_file, source_file)
        resource_yaml.update_cdf_resource(cdf_resource)

        added = {".".join(key): value.cdf_value for key, value in resource_yaml.items() if value.is_added}
        changed = {".".join(key): value.cdf_value for key, value in resource_yaml.items() if value.is_changed}
        cannot_change = {
            ".".join(key): value.cdf_value for key, value in resource_yaml.items() if value.is_cannot_change
        }

        assert added == expected["added"]
        assert changed == expected["changed"]
        assert cannot_change == expected["cannot_change"]
