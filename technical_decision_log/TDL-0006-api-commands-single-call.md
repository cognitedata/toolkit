# TDL-0006: API Commands Execute a Single API Call

**Date:** 2026-11-10

## What

The `cdf api` commands give users direct access to the CDF API, for example `cdf api functions list`. It is tempting
to make them more convenient by combining several requests in one command, such as calling a function, waiting for
it to finish, and fetching its response and logs. That turns `cdf api` into a second implementation of workflows that
already exist in other commands, for example `cdf dev run function`.

## Decision

A `cdf api` command executes exactly one API call. There are two exceptions, where extra calls are allowed only to
prepare that single call:

1. **External ID to internal ID translation.** If the endpoint only accepts an internal ID, the command may look up
   the internal ID from an external ID first. For example, `POST /functions/{id}/call` requires the internal ID.
2. **Nonce.** If the service requires a nonce, the command may create a session to get one. For example, calling a
   function or running a transformation or workflow.

Anything beyond this, such as polling until a job completes or combining the results of several endpoints, belongs in
another command, for example under `cdf dev`.

## Why

One call per command keeps `cdf api` predictable and easy to reason about: what a command does, which permissions it
needs, and how it fails maps directly to one API endpoint. It also avoids having the same multi-step functionality in
two places. The two exceptions are plumbing the user would otherwise have to do by hand without changing what the
command does, so allowing them keeps the commands usable without breaking the one-to-one mapping to the API.
