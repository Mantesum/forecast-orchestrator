# Changelog

All notable changes to this project are documented here.

## 0.1.0 - Unreleased

- Add explicit-cycle discovery for one active GFS, IFS, or AIFS source.
- Add profile-specific ingest and Zarr template selection with `full_energy` as the default.
- Add durable SQLite job state and a single-process lock.
- Chain `forecast-ingest plan/download` with `forecast-zarr plan/convert/validate`.
- Verify the manifest-to-READY SHA-256 provenance chain before publication.
- Atomically publish a relative `current.json` pointer and optional `current.zarr` symlink.
- Add guarded GRIB, Zarr, and failed-staging retention with an API-reader grace period.
- Add structured journald output, systemd units, and Ubuntu/NFS deployment guidance.
- Treat NOMADS 403/404 availability responses as a pending GFS cycle and fall back to the
  latest complete cycle.
- Allow the systemd oneshot up to 14 hours for full ingest, conversion, and validation.
