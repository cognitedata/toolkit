# TDL-0006: API Commands Map to One API Endpoint

**Date:** 2026-11-10

## What

The `cdf api` commands give users direct access to the CDF API, for example `cdf api functions list`. It is tempting
to make these commands more convenient by chaining several API calls, for example calling a function, waiting for it
to finish, and then fetching its response and logs. It is also tempting to name each command in whatever way reads
best on its own, or to support every endpoint, including those that create or change resources.

## Decision

### Only read and run operations

`cdf api` supports only operations that read resources, such as `list`, `retrieve`, and `status`, or that run them,
such as calling a function. It does not create or upsert resources. That is done through the declarative
`cdf build` and `cdf deploy` cycle.

### One API call per command

Each `cdf api` command makes exactly one API call. There are two exceptions. Both only prepare the input for that one
call:

1. **External ID lookup.** If the endpoint accepts only an internal ID, the command may first look up the internal ID
   from the external ID. For example, `POST /functions/{id}/call` requires the internal ID.
2. **Session nonce.** If the endpoint requires a nonce, the command may first create a session to get one. For
   example, calling a function or running a transformation or workflow.

Anything else, such as waiting for a job to complete or combining results from several endpoints, belongs in a
different command, for example under `cdf dev`.

### An object followed by a verb

A `cdf api` command is an object followed by a verb: `cdf api <object> <verb>`. The object must have the same name as
the API resource, for example `functions` for `/functions` and `instances` for `/models/instances`.

The common verbs are:

- `list` returns multiple objects, optionally filtered.
- `retrieve` returns a single object, identified by its external ID with `--external-id`.
- `status` returns the status of the object if such an endpoint exists.

Objects are always identified by external ID, never by internal ID. For example:

| Command                                            | What it does                                        |
|----------------------------------------------------|-----------------------------------------------------|
| `cdf api functions list --status Ready`            | Lists functions with the deployment status `Ready`. |
| `cdf api functions retrieve --external-id my_func` | Retrieves the function `my_func`.                   |
| `cdf api functions status`                         | Shows whether Functions is activated.               |
| `cdf api instances list --view space:View/v1`      | Lists instances with properties from the view.      |

## Why

- **Single source of truth.** Resources are defined in configuration files and deployed with `cdf build` and
  `cdf deploy`. Creating or upserting resources through `cdf api` would make CDF drift from that configuration.
- **Predictability.** Each command maps to one endpoint, so its behavior, required permissions, and errors are those of
  that endpoint. Because every command is an object followed by a verb, and the object has the same name as the API
  resource, users can guess a command from the API documentation without looking it up.
- **Performance.** Chained calls make a command slower, especially when it waits for a job to complete. With one call,
  the command takes as long as the endpoint does.
- **Transparency.** When a command chains several calls, users cannot see which calls it made, which one was slow, or
  which one failed. With one call, it is clear which endpoint the command calls.

The two exceptions to the one-call rule do not undermine this. Each is a single, fast call that users would otherwise
have to make by hand, and neither changes what the command does.
