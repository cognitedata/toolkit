# TDL-0003: Interactive mode change on patch

**Date:** 2026-02-24

## What

Most Toolkit commands have an interactive mode. This means that when you run the command without any arguments,
it will ask you to select what arguments you want. For example, when you run `cdf data download raw/instances`,
it will ask you to select which instances you want to download.

## Decision

Changing the interactive mode of a command can be done in a patch release.

## Why

Interactive mode will never run in a CI/CD pipeline, thus it cannot break any pipelines. Allowing it to change
enables us to make improvements faster, thus this is low risk for incidents and good reward in terms of improving
the user experience.
