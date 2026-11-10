# TDL-0006: One API Call per API Command

**Date:** 2026-11-10

## What

The `cdf api` commands give users direct access to the CDF API, for example `cdf api functions list`. It is tempting
to make these commands more convenient by chaining several API calls, for example calling a function, waiting for it
to finish, and then fetching its response and logs.

## Decision

Each `cdf api` command makes exactly one API call. There are two exceptions. Both only prepare the input for that one
call:

1. **External ID lookup.** If the endpoint accepts only an internal ID, the command may first look up the internal ID
   from the external ID. For example, `POST /functions/{id}/call` requires the internal ID.
2. **Session nonce.** If the endpoint requires a nonce, the command may first create a session to get one. For
   example, calling a function or running a transformation or workflow.

Anything else, such as waiting for a job to complete or combining results from several endpoints, belongs in a
different command, for example under `cdf dev`.

## Why

- **Predictability.** Each command maps to one endpoint, so its behavior, required permissions, and errors are those of
  that endpoint.
- **Performance.** Chained calls make a command slower, especially when it waits for a job to complete. With one call,
  the command takes as long as the endpoint does.
- **Transparency.** When a command chains several calls, users cannot see which calls it made, which one was slow, or
  which one failed. With one call, there is nothing hidden.

The two exceptions do not undermine this. Each is a single, fast call that users would otherwise have to make by hand,
and neither changes what the command does.
