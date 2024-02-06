from cognite_toolkit.cdf_tk.pull import ResourceYAML


class TestResourceYAML:
    demo_file = """externalId: tr_pump_asset_hierarchy-load-collections_pump
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

    def test_load_and_dump_persist_comments(self) -> str:

        resource_yaml = ResourceYAML.load(self.demo_file)

        dumped = resource_yaml.dump_yaml_with_comments()

        assert dumped == self.demo_file
