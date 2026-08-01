# Architecture and safety model

`forecast-orchestrator` coordinates two independent, versioned applications. It does not
decode GRIB or write Zarr arrays itself.

For one configured UTC cycle it performs:

1. Write an immutable job-specific ingest YAML with an explicit `run`.
2. Run `forecast-ingest plan --json`. A missing final forecast step means "wait", not failure.
3. Run `forecast-ingest download`; require a complete source `manifest.json`.
4. Write a job-specific Zarr YAML pointing to that exact immutable input directory.
5. Run `forecast-zarr plan`, `convert`, and then the independent `validate` command.
6. Verify `READY.json`, source-manifest SHA-256, provider, model, run, variables, and times.
7. Atomically replace `current.json` and the optional relative `current.zarr` symlink.
8. Only now make the source GRIB eligible for retention cleanup.

SQLite records every attempted cycle. The unique source/profile/run identity and the child
applications' immutable IDs make recovery idempotent. A process lock prevents overlapping
timer invocations.

## Failure boundaries

- A publication probe failure leaves all data unchanged.
- A download failure leaves resumable `.part` files owned by `forecast-ingest`.
- A conversion failure leaves resumable `.staging` owned by `forecast-zarr`.
- A validation failure cannot replace `current.json`.
- A pointer publication happens before cleanup.
- Cleanup only traverses manifest-linked paths under configured managed roots.
- Cleanup failure is reported but never revokes a successfully published forecast.

The API must treat `current.json` as the authority and open its immutable relative `store`
path. It must not repeatedly resolve the moving symlink during one request.

