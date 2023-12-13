# Module: cdf_auth_readwrite_all

This module is used to create:

1. a group with read write access to everything in a CDF project (for a CI/CD pipeline)
2. a group with read-only access (for viewing configurations from UI)
3. a default group for infield that currently is needed to see the UI config menu

This module can be used for production as is.

It currently uses the following global configuration variables:
demo_readwrite_source_id and demo_readonly_source_id.