# Module: cdf_auth_readwrite_all

This module contains two groups that are used to grant access to all resources in a CDF project. This
should **never** be used for production projects, as it grants read-write access to all resources in the project.
However, it is useful for sandbox projects to allow members of the `gp_admin_read_write` group to have full access.
It can also be used for demo projects where you want to give full read access `gp_admin_readonly` access to
all resources to a group of users.

## Managed resources

This module manages the following resources:

1. a group with read-write access (`gp_admin_read_write`) to everything in a CDF project.
2. a group with read-only access `gp_admin_readonly` (for viewing configurations from UI).

## Variables

The following variables are required and defined in this module:

| Variable            | Description                                                                                          |
|---------------------|------------------------------------------------------------------------------------------------------|
| readwrite_source_id | The source ID of the group that should be granted read-write access to all resources in the project. |
| readonly_source_id  | The source ID of the group that should be granted read-only access to all resources in the project.  |
