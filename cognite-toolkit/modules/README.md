# Main modules folder

Modules prefixed by `cdf_` are managed by Cognite and should not
be modified. They live in this directory. You should put your own modules in the
local_modules/ directory.

Each module should have a config.yaml file that contains variables that are used in the module. The
sub-directories in each module correspond to the different resources in CDF.

See the [module and package documentation](../docs/overview.md) for an introduction.