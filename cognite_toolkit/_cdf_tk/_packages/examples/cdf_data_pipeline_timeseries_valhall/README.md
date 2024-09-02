# cdf_data_pipeline_time_series_valhall

This module relies on these example data modules being loaded:

- cdf_oid_example_data
- cdf_data_pipeline_asset_valhall

The module creates a simple data pipeline for contextualization of time series metadata with
the asset data based on the OID example module. In addition the module contains and example
OPC-UA extractor configuration. Example configuration can be used in combination with
[Prosys OPC UA Simulation Server](https://prosysopc.com/products/opc-ua-simulation-server/)
for testing the Cognite OPC UA extractor.

The contextualization function can also be used in migration of time series to asset mappings
by usage of the RAW table `contextualization_manual_input` described below.
By reading the external ID of the time series and corresponding asset mapping into this
table, the contextualization job will recreate the mapping in your new project.

The contextualization function will then in `normal`operation used the RAW
table `contextualization_manual_input` as input to the contextualization where the
automated process not finds the right relationship. Content in the table
`contextualization_manual_input` will then have priority over any automatic/machine mapping
When mapped the time series metadata will be updated with a property `TS_CONTEXTUALIZED`
describing the mapping. Next time running the contextualization only new or
old time series not containing this property will be used as input and tried to
contextualized.  All content will only be remapped if you are running with configuration `runAll = True`.
This will remap all manual mappings and run entity mapping on all other time series and
delete and recreate content in the result table `contextualization_good`

## test data

The data for the mapping are from the module `cdf_oid_example_data` is a small data set
from [Open IndustrialData](https://learn.cognite.com/open-industrial-data), the Valhall platform

Related to integration with the OPC-UA simulator you can modify the names of the time series
that the simulator will use making it possible to run the
contextualization process on it by modifying the names used on the variable in the OPC-UA simulator UI, ex:
![image](https://github.com/cognitedata/toolkit/assets/31886431/34525295-1cd7-4f11-8aec-bc4f9db0bc8b)

Note that the external ID of the time series from the simulator is related to the setup in the server,
and will not match the example data from OID. So the time series from the simulator server should not
use the same names as we have in the OID example data.

## Managed resources

This module manages the following resources:

1. auth:
   - Name: `gp_timeseries_oid_extractor`
     - Content: Authorization group used by the extractor writing data to CDF time series and RAW,
       extraction pipeline and update of extraction pipeline run
   - Name: `gp_timeseries_oid_processing`
     - Content: Authorization group used for contextualization of time series function, read/write
       to raw and pipeline runs
   - Name: `gp_timeseries_oid_read`
     - Content: Authorization group used by the users reading time series and data points

2. data set:
   - ID: `ds_timeseries_oid`
     - Content: Data lineage, used Links to used extraction pipelines, transformation and raw tables

3. extraction pipeline:
   - ID: `ep_src_timeseries_oid_opcua`
     - Content: Documentation and configuration example for the opcua extractor with connection to
       OPC-UA extractor simulator
   - ID: `ep_ctx_timeseries_oid_opcua_asset`
     - Content: Documentation and configuration for a CDF function running example PI data and OPC-UA
       time series contextualization against assets (see function form more description)

4. functions:
   - ID: `fn_context_timeseries_oid_opcua_asset`

      Read configuration and start process by

        1. Read RAW table with manual mappings and extract all rows not contextualized.
        2. Apply manual mappings from TS to Asset - this will overwrite any existing mapping
        3. Read all time series not matched (or all if runAll is True)
        4. Read all assets
        5. Run ML contextualization to match TS -> Assets
        6. Update TS with mapping
        7. Write results matched (good) not matched (bad) to RAW
        8. Output in good/bad table can then be used in workflow to update manual mappings

5. raw:
   - db/table: `timeseries_oid_opcua/contextualization_good`
     - Content: result from contextualization with mappings that was added to CDF between TS and assets.
       Table can be used for quality control and input to manual tuning
   - db/table: `timeseries_oid_opcua/contextualization_bad`
     - Content: result from contextualization with mappings NOT added to CDF between TS and assets.
       Table can be used for quality control and input to manual tuning
   - db/table: `timeseries_oid_opcua/contextualization_manual_input`
     - Content: manual input to contextualization process. Table can be used as:
       - a migration table read existing mappings from old project and insert to this table
         to re-map in new project
       - manual tuning based on results from process - wrong mappings in good/bad table can be
         corrected by adding it to this table

### Illustration of the time series data pipeline

![image](https://github.com/cognitedata/toolkit/assets/31886431/5da18c06-8324-4f9f-a9e9-ce61e87e1a51)

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| location_name | The location for your data, name used in all resource type related to data pipeline. We use oid (Open Industrial Data) |
| source_name | The name of the source making it possible to identify where the data originates from, ex: 'workmate', 'sap', 'oracle',..|
| timeseries_dataset | The name of data set used for time series in this example, must correspond to name used in example data|
| opcua_endpoint_url |  endpoint for OPC-UA server, simulator example: `opc.tcp://<host>:53530/OPCUA/SimulationServer`|
| opcua_id_prefix |  prefix used for time series external ID, ex: `opc-ua:`|
| opcua_root_namespace_uri |  Namespace URI, simulator example : `http://www.prosysopc.com/OPCUA/SimulationNodes/`|
| opcua_root_node_id |  OPC-UA root node, simulator example : `s=85/0:Simulation`|
| timeseries_location_extractor_group_source_id | Object/ Source ID for security group in IdP. Used to run integration/extractor|
| timeseries_location_processing_group_source_id | Object/ Source ID for security group in IdP. Used to run CDF processing/contextualization|
| timeseries_location_read_group_source_id | Object/ Source ID for security group in IdP. Used to read file data|

## Usage

You should copy and rename an example module into the `custom_modules` directory (remove any `cdf_` prefixes) and make
your own modifications. You should then update the `selected_modules_and_packages:` section in your `config.[env].yaml`
file to install the module.

`NOTE: Using Cognite Functions to run workloads will be limited by the underlying resources
 in the cloud provider functions. Hence processing large volumes of time series & asset data
 will not be optimal in a CDF function since it will time out and fail.`

See [Using Templates](https://developer.cognite.com/sdks/toolkit/templates)
