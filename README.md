# forecast-orchestrator

`forecast-orchestrator` safely chains
[`forecast-ingest`](https://github.com/Mantesum/forecast-ingest) and
[`forecast-zarr-processor`](https://github.com/Mantesum/forecast-zarr-processor).
It discovers configured model cycles, downloads and validates GRIB, converts one immutable
run to Zarr v3, validates it again, atomically exposes `current.json`, and only then applies
retention.

The default operational model is one active source, the `full_energy` profile, four GFS
cycles per day, and a ten-minute publication poll. Ubuntu `systemd` runs the checks; a
read-only NFS export lets a Django API on another VM consume the latest complete forecast.

## Safety guarantees

- The moving API pointer never references staging or an unvalidated store.
- An old forecast stays current after any download, conversion, or validation failure.
- The pointer contains a relative immutable store path and a `READY.json` checksum.
- A superseded Zarr observes a grace period before deletion.
- GRIB deletion requires a complete manifest linked to a successfully published job.
- All recursive deletion targets are constrained to configured managed roots.
- SQLite state and deterministic child IDs make retries idempotent.

See [architecture](docs/architecture.md) and [Ubuntu/NFS deployment](docs/ubuntu-nfs.md).

## Install on Ubuntu

Install Python 3.12+, Git, and `uv`. Install the two child projects in their own locked virtual
environments, then install this repository:

```bash
git clone https://github.com/Mantesum/forecast-orchestrator.git
cd forecast-orchestrator
uv sync --frozen
```

Create the service account and directories:

```bash
sudo useradd --system --home /var/lib/forecast-orchestrator --shell /usr/sbin/nologin forecast-orchestrator
sudo install -d -o forecast-orchestrator -g forecast-orchestrator -m 0750 /var/lib/forecast-orchestrator
sudo install -d -o forecast-orchestrator -g forecast-orchestrator -m 0750 /srv/forecast-public
sudo install -d -m 0755 /etc/forecast-orchestrator
sudo cp configs/orchestrator.example.yaml /etc/forecast-orchestrator/orchestrator.yaml
```

Copy the selected ingest/Zarr templates to the paths referenced by the orchestrator YAML.
Grant the service account write access to the configured ingest data directory and publication
root. The two executable paths may point at their projects' separate virtual environments.

Check the installation without contacting a forecast provider:

```bash
uv run forecast-orchestrator doctor --config /etc/forecast-orchestrator/orchestrator.yaml
uv run forecast-orchestrator status --config /etc/forecast-orchestrator/orchestrator.yaml
```

Run one discovery attempt manually:

```bash
uv run forecast-orchestrator run-once --config /etc/forecast-orchestrator/orchestrator.yaml
```

## systemd

Review all paths and resource limits before installation:

```bash
sudo cp systemd/forecast-orchestrator.service systemd/forecast-orchestrator.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now forecast-orchestrator.timer
systemctl status forecast-orchestrator.timer
journalctl -u forecast-orchestrator.service
```

The timer checks every ten minutes. `run-once` processes at most one complete, unprocessed
cycle and exits successfully with `status: waiting` when publication is not complete.

## Disk sizing

Safe replacement necessarily holds the old current Zarr while the new GRIB and Zarr staging
exist. For the global GFS `full_energy` profile, size the filesystem for the current Zarr,
the complete replacement Zarr, raw GRIB, conversion temporary space, and the configured free
space reserve at the same time. A 40 GiB Zarr managed-total limit is generally insufficient
when one near-maximum 26 GiB store already exists; review the actual plans and provision a
substantially larger volume (often at least 80–100 GiB for this profile).

The orchestrator deliberately does not delete the current forecast to make a new plan fit.
If a child budget check fails, increase storage or reduce the selected profile/horizon.

## Configuration

`active_source` selects one source. `profile` selects one of that source's configured profile
pairs. GFS examples include `weather`, `wind_energy`, `solar_energy`, and `full_energy`.

ECMWF IFS and AIFS do not expose every GFS full-energy field. Unsupported variables fail
planning by default, preventing a partial dataset from being advertised under a complete
profile name. Provide matched ingest and Zarr templates for the ECMWF subset you intend to
serve.

`cycles_utc` controls successful runs per day. `[0, 6, 12, 18]` means four; `[0, 12]` means
two. The frequent timer is only an availability poll and never duplicates an existing cycle.

## CLI

```text
forecast-orchestrator doctor --config CONFIG
forecast-orchestrator run-once --config CONFIG
forecast-orchestrator status --config CONFIG
forecast-orchestrator validate --config CONFIG
forecast-orchestrator cleanup --config CONFIG
```

Commands print machine-readable JSON on stdout. Structured operational events go to stderr
for journald.

## Development

```bash
uv sync --extra dev
uv run ruff format --check .
uv run ruff check .
uv run mypy --strict src
uv run pytest
```

## License

Apache-2.0. Forecast data retain their providers' licenses and attribution requirements.
