# Module: cdf_auth_readwrite_all

This is a foundational module used by the `cdf-tk` tool as the default
auth module for read-write access to all CDF resources for the tool itself (admin or CI/CD pipeline),
as well as default read-only access for admin access in the UI.

This structure is based on the concept of ONLY the tool having write access to the entities
that are controlled by the templates. Everybody else should either have no access or read-only access.

## Managed resources

This module manages the following resources:

1. a group with read-write access (`gp_cicd_all_read_write`) to everything in a CDF project (for `cdf-tk` as an admin
   tool or through a CI/CD pipeline).
2. a group with read-only access `gp_cicd_all_read_only` (for viewing configurations from UI).

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
|readwrite_source_id| The source ID of the group that should be granted read-write access to all resources in the project. |
|readonly_source_id| The source ID of the group that should be granted read-only access to all resources in the project.|

## Usage

The `gp_cicd_all_read_write` group is used default by the `cdf-tk auth verify` command to verify correct access to
resources in a project. The groups are default part of several packages that are created by the `cdf-tk` tool.

If you have different needs for the readwrite and readonly groups, you can copy this module into `custom_modules`, rename
it (remove the cdf_ prefix), and change which modules are deployed in your `environments.yaml` file. You can also
use the `cdf-tk verify --group-file=/path/to/group.yaml` command to switch out the default group file with your own.
