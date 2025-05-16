# cdf_inrobot_location

This module contains location specific configurations for InRobot. This is the default location.
To support multiple locations, copy this module and modify the configurations. Remember to
rename the module name to e.g. `inrobot_location_<location_name>`.

## Auth

The module creates three groups that need three matching groups in the identity provider that the CDF
project is configured with. The groups are:

- Admin role. This role has access to create 3D models, create assets, create new video rooms, etc.
- User role. This is the standard role for most users. These users can interact with the robot and see all data
  associated with the robot.
- A robot user role - this is given to the robot and includes the ability to create labels, write to files, write to
  FDM (checklists, checklist items). Any new robot in a given location must have a new user group. Similarly,
  the same robot in a new location must have a new user group.

All the users read from robot-specific data sets. This means that when a new robot is added, the dataset scopes
for the users must be updated as well.

The source ids from the groups in the identity provider should be set in [./default.config.yaml](default.config.yaml).

## Data models

There are two spaces created in this module: one space for InRobot to store app data and one space for
data from source systems, like assets, activities/work orders etc.

## Robotics

Each location must have three associated robotics-specific resources created: a Map, a Frame, and a Location. These are
robotics-api specific concepts and are required for the robot to understand its environment. These require a threeD
model name, type (THREEDMODEL or POINTCLOUD) revisionId, and modelId in addition to the root asset external id.
