# Operations

## Normal result states

`run-once` returns `ready` after publishing one new run or `waiting` when no newer complete
cycle is available. Both are successful systemd outcomes. Expected operational failures have
stable non-zero exits:

- 2: invalid configuration;
- 3: another instance holds the process lock;
- 4: a child executable is missing;
- 5: ingest planning or download failed;
- 6: Zarr planning or conversion failed;
- 7: validation failed;
- 8: atomic publication failed;
- 9: an explicitly requested retention operation failed.

Automatic cleanup errors are warnings after publication: they must never make a ready current
forecast disappear.

Retention is also evaluated by `waiting` timer invocations, so a superseded Zarr is removed
soon after its grace period rather than remaining until the next six-hour model cycle.

## Commands

```bash
forecast-orchestrator doctor --config /etc/forecast-orchestrator/orchestrator.yaml
forecast-orchestrator status --config /etc/forecast-orchestrator/orchestrator.yaml
forecast-orchestrator validate --config /etc/forecast-orchestrator/orchestrator.yaml
forecast-orchestrator cleanup --config /etc/forecast-orchestrator/orchestrator.yaml
```

Use `journalctl -u forecast-orchestrator.service` for orchestration events and the streamed
stderr logs of both child applications.

## Alerts

Alert when:

- no `current.json` exists after initial deployment;
- its `run_utc` exceeds the product-specific freshness threshold;
- three consecutive timer invocations fail;
- free disk falls below the stricter child-application budget;
- `.staging` makes no progress beyond the configured TTL;
- `validate` fails for the current immutable store;
- NFS is unavailable on the API VM.

## Capacity planning

At publication time the old API-visible Zarr, the complete new Zarr, source GRIB, temporary
conversion data, and minimum-free-space reserve coexist. Configure both child applications
for that peak, not merely for one final store. Keeping the old current store is a hard safety
requirement and is never bypassed to satisfy a disk budget.

## Recovery

Do not delete `.part` or `.staging` immediately after a failure. The child applications own
and resume them. Re-run the timer unit after correcting the cause. Deterministic job YAML is
kept below the state directory for diagnosis.

On Windows development hosts, a stale lock is removed manually because Windows does not offer
the POSIX signal-zero liveness probe used on production Ubuntu.
