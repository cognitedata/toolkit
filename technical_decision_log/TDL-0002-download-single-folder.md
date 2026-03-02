# TDL-0002: Download Commands Use Single Folder

**Date:** 2026-02-24

## What

All download commands in Toolkit, `cdf data download raw/instances/...`, should have an argument `--output-dir` that
specifies the destination folder for downloaded data, which defaults to `/data`.

## Decision

All data selected by the user should be placed in a subdirectory of the specified output directory. It should have a
structure such as

```shell
data/
└── my_selected_data/
    ├── resoruces/
    ...
    ├── some.Manifest.yaml
    └── somedata.RawRows.ndjson
```

## Why

First, using a subdirectory of the specified output directory means that if the user runs the command multiple
times and selects different data, the data will automatically go in different folders without the user having to
specify the output directory each time. Second, in an earlier version of Toolkit, the selected data could be placed
in different folders. This was confusing for users, and caused issues in particular with instances as they typically
have dependencies with each other. So when you upload multiple directories, the user had to know the dependencies
between the data. [Ref](https://github.com/cognitedata/toolkit/pull/2544)

Essentially: "Running download once should result in one directory with data".
