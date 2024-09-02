# cdf_inrobot_common

This module contains shared configurations across multiple locations for InRobot.

## Auth

The module creates one group that needs one matching group in the identity provider that the CDF
project is configured with. The group is:

- Run Function User. This role is used by our functions when they need to interact with CDF, for example, to add
  annotations to images, to create timeseries, etc. This makes reference to the robot's data set, so each new robot
  requires a new run function user.

## Data models

There is one space created in this module called cognite_app_data. This is the space where user and user profile
data is stored. We also populate the APM_Config node with our default config, including the information about the
location. If new locations are added, this node must be updated to include the new locations.

## Data sets

This module creates a new data set for the robot. All the robot data in CDF will be stored in this data set. The
external id is specified in the config.yaml file, and this data_set is referred to in many other modules. Any new
robots must have a new data set.

## Functions

This module contains four (4) functions that we deploy: Contextualize Robot Data, Gauge Reading, Get IR Data from
IR Raw, and ThreeSixty. These are functions that run every minute on files with certain labels that
the robot uploads to CDF.

Because the function itself must be stored in a dataset and we use the dataset of the robot, a new function must be
defined for each robot in the robots.functions.yaml file. Additionally, most schedules use the robot data set id as a
data parameters - this means that a new schedule must be created for every new robot.

## Labels

This module creates two labels that are needed for InRobot to work: robot_gauge and read_ir. These are scoped to
the robot's data set. This means that new labels must be created for each additional robot.

## Robotics

This module creates some robotics specific resources: RobotCapability and DataPostProcessing.

Currently we support the following RobotCapability resources: acoustic_video, pt_ir_video, ptz_ir, ptz_video,
ptz, threesixty, and threesixty_video.

Currently we support the following DataPostProcessing resources:
process_threesixty, read_dial_gauge, read_digital_gauge, read_level_gauge, read_valve.

If there are any of these robot capabilities you do not want, you can remove the YAML file. Note that the
process_threesixty data post processing requires the threesixty capability to be present. Similarly,
the various gauge data post processing options require the ptz robot capability.
