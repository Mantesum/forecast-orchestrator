# Contributing

Contributions are welcome through focused GitHub issues and pull requests.

## Development setup

Use Python 3.12+ and `uv`:

```bash
uv sync --extra dev
uv run ruff format --check src tests
uv run ruff check src tests
uv run mypy --strict src
uv run pytest
```

Tests must not contact live forecast providers. Use a fake command runner for orchestration
tests and reserve real-provider checks for explicit manual smoke tests.

Changes to publication or retention require tests proving that an incomplete or older store
cannot replace the current forecast and that paths outside managed roots cannot be deleted.

Do not add provider credentials, private addresses, local configuration files, GRIB, or Zarr
data to the repository.

