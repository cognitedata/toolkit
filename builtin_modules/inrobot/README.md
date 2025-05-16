# InRobot Module

This module allows you to quickly set up an InRobot project. There are a few pre-requisites:

- You must have an asset heirarchy already set up. Specifically you will need the external id of the root asset for
  your location.
- You must have a 3D model already uploaded in Fusion. Note its model id, its revision id, and its name.
- You must have created 4 groups in your source system: Users*<Location>, Admin*<Location>, Robot*1*<Location>,
 Run_Function_User. The naming does not matter specifically, but you may need to add more locations and/or robots in
 the future, so it would be ideal to name the groups accordingly.
- You must have created an app registration for the robot, and added the app registration to the robot user group. Note
  the client id and the secret for the robot app registration.
- You must have already activated the functions service for your project. This can be done in Fusion, and can take up
  to 2 hours to become activated.

This module is meant to be used in conjunction with the toolkit common module and the cdf_apm_base module.

For now, until the next version release of toolkit, you must also enable the following toolkit feature flags:

cdf features set fun-schedule --enable
cdf features set robotics --enable

## Configuration Variables

Specific inrobot variables you will need to define in your config YAML file:

| Variable Name                             | Description                                                                                                                  |
|-------------------------------------------|------------------------------------------------------------------------------------------------------------------------------|
| `first_root_asset_external_id`            | This is the asset external ID for your root asset.                                                                           |
| `first_location`                          | A human readable name that will be included as part of different location-specific spaces and groups.                        |
| `inrobot_admin_first_location_source_id`  | The ID for the admin group for the location.                                                                                 |
| `inrobot_users_first_location_source_id`  | The ID for the users group for the location.                                                                                 |
| `robot_1_first_location_source_id`        | The ID for the robot group for the location.                                                                                 |
| `run_function_user_group_source_id`       | The ID for the run function group.                                                                                           |
| `run_function_client_id`                  | The run function client ID (app registration in Azure).                                                                      |
| `run_function_secret`                     | The secret for the run function app registration. This will be stored in your env file and should be referenced securely.    |
| `robot_1_dataset_ext_id`                  | This is the data set for your robot. You can give this whatever value you want.                                              |
| `three_d_model_name`                      | The name of the 3D model as named in Fusion.                                                                                 |
| `three_d_type`                            | The type of 3D model. This will be either `THREEDMODEL` or `POINTCLOUD`.                                                     |
| `three_d_model_id`                        | The model ID of your 3D model.                                                                                               |
| `three_d_revision_id`                     | The revision ID of your 3D model.                                                                                            |
