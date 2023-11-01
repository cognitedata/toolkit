# Main module folder

Most modules should live in this directory. Modules prefixed by `cdf_` are managed by Cognite and should not
be modified. In each module directory, you will find a config.yaml that you can edit to custimize the
module.

You are free to add your own modules to this directory as long as you don't use the `cdf_` prefix.
Each module should have a config.yaml file that contains variables that are used in the module. The
sub-directories in each module correspond to the different resources in CDF.

See the [module and package documentation](../docs/overview.md) for an introduction.