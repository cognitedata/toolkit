# TDL-0001: Drop Package Selection in config.yaml Selection

**Date:** 2026-02-23

## What

In version 0.7 and earlier, the user can define packages as a group of modules in the `cdf.toml` file.

```toml title="cdf.toml"
...
[modules.packages]
my_package = ["module1", "module2", "module3"]
my_other_package = ["module4", "module5"]

```

And in the `config.<env>.yaml` file use this package name to select the modules to build and deploy

```yaml title="config.dev.yaml"
environment:
  name: dev
  project: my_project
  validation-type: dev
  selected:
  - my_package
  - my_other_package
```

## Decision

We will drop support for package selection from `v0.8` onwards.

## Why

We introduced this in the first version of Toolkit, later we introduced selection by path, which made this
package selection less relevant.

```yaml title="config.dev.yaml"
environment:
  name: dev
  project: my_project
  validation-type: dev
  selected:
  - modules/path/all_modules_in_directory
```

In addition, it introduces a new concept, `packages` which can easily be confused with `deplyoment package`.

This is an undocumented feature, which is not used by any of our users as far as we know, thus we
will drop it from `v0.8` onwards.
