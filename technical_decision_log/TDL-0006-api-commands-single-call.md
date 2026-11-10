# TDL-0006: API Commands Map to One API Endpoint

**Date:** 2026-11-10

## What

The `cdf api` commands give users direct access to the CDF API, for example `cdf api functions list`. It is tempting
to make these commands more convenient by chaining several API calls, for example calling a function, waiting for it
to finish, and then fetching its response and logs. It is also tempting to give commands names that read well on the
command line but differ from the API, or to support every endpoint, including those that create or change resources.

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

### The command reflects the URL path

The words of a `cdf api` command generally follow the segments of the endpoint's URL path, in the same order: first
the object, then the verb. Path parameters, such as `{id}`, become options instead of command words. Objects are
always identified by external ID, with `--external-id`, never by internal ID.

The standard verbs are `list` and `retrieve`, and they always come after the object:

- `list` is for listing objects, `POST /<object>/list` or `GET /<object>`. If the search endpoint is a better fit for
  listing, `list` may use `POST /<object>/search` instead, as `cdf api instances list` does.
- `retrieve` is for retrieving objects by ID, `POST /<object>/byids`.

For all other endpoints, the last segment of the URL path is the verb. For example:

| Endpoint                           | Command                                 |
|------------------------------------|-----------------------------------------|
| `POST /functions/list`             | `cdf api functions list`                |
| `POST /functions/byids`            | `cdf api functions retrieve`            |
| `GET /functions/status`            | `cdf api functions status`              |
| `POST /models/instances/search`    | `cdf api instances list`                |
| `POST /models/instances/byids`     | `cdf api instances retrieve`            |
| `POST /models/instances/aggregate` | `cdf api instances aggregate`           |
| `POST /models/instances/query`     | `cdf api instances query`               |

## Why

- **Single source of truth.** Resources are defined in configuration files and deployed with `cdf build` and
  `cdf deploy`. Creating or upserting resources through `cdf api` would make CDF drift from that configuration.
- **Predictability.** Each command maps to one endpoint, so its behavior, required permissions, and errors are those of
  that endpoint. When the command also mirrors the URL path, users can go from the API documentation to the command,
  and back, without a lookup table.
- **Performance.** Chained calls make a command slower, especially when it waits for a job to complete. With one call,
  the command takes as long as the endpoint does.
- **Transparency.** When a command chains several calls, users cannot see which calls it made, which one was slow, or
  which one failed. With one call, and a name that matches its URL path, it is clear which endpoint the command calls.

The two exceptions to the one-call rule do not undermine this. Each is a single, fast call that users would otherwise
have to make by hand, and neither changes what the command does.
