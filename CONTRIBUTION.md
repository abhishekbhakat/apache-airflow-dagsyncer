<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Contributing to apache-airflow-dagsyncer

## Development setup

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12+.

```bash
git clone https://github.com/abhishekbhakat/apache-airflow-dagsyncer.git
cd apache-airflow-dagsyncer
uv sync --dev
```

The dev group installs `apache-airflow`, `ruff`, `mypy`, and `pytest`.

## Project layout

| Path                              | Purpose                                        |
|-----------------------------------|------------------------------------------------|
| `src/airflow/dagsyncer/protocol.py` | Wire protocol dataclasses (stdlib only)      |
| `src/airflow/dagsyncer/parse.py`  | One-shot DagProcessor run + SQLite extraction  |
| `src/airflow/dagsyncer/client.py` | HTTP push client (stdlib only)                 |
| `src/airflow/dagsyncer/server.py` | Listener HTTP server (stdlib only)             |
| `src/airflow/dagsyncer/ingest.py` | Control-plane ingestion (requires Airflow)     |
| `src/airflow/dagsyncer/cli.py`    | `push` / `listen` commands                     |
| `PROTOCOL.md`                     | Wire protocol specification                    |

`airflow.dagsyncer` is a PEP 420 namespace package under Airflow's `airflow`
namespace. The core package must stay free of runtime dependencies; anything
importing Airflow belongs behind the `[listen]` extra (see `ingest.py`).

## Checks

Run all of these before opening a PR (CI enforces them):

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q
```

Ruff and mypy configuration live in `pyproject.toml`; the ruff rules are
adapted from apache/airflow.

## Continuous integration

`.github/workflows/ci.yml` runs on every PR and push to `main`:

- `lint`: ruff check, ruff format check, mypy (strict)
- `test`: pytest against the locked (highest) resolution
- `test-lowest`: pytest with `--resolution lowest-direct` to prove the
  declared `apache-airflow>=3.2.0` floor
- `build`: `uv build` and a smoke run of the built wheel

## Release process

Releases are tag-driven and published to PyPI via trusted publishing
(`.github/workflows/release.yml`). No API tokens are stored in the repo.

Release candidates use PEP 440 pre-release versions (for example
`0.0.1rc2`); pip only installs them with `--pre` or an exact pin.

1. Bump the version in **both** places, keeping them identical:
   - `pyproject.toml` (`[project] version`)
   - `src/airflow/dagsyncer/__init__.py` (`__version__`)
2. Update `CHANGELOG.md`.
3. Commit and push, then tag with the exact version string:

   ```bash
   git tag 0.0.1rc2
   git push origin 0.0.1rc2
   ```

4. The `Release` workflow triggers on the tag:
   - verifies the tag matches `uv version --short` (mismatch fails the build)
   - builds the sdist and wheel, smoke-tests the wheel
   - publishes to PyPI with `pypa/gh-action-pypi-publish` via OIDC

### One-time PyPI setup

On <https://pypi.org/manage/project/apache-airflow-dagsyncer/settings/publishing/>,
add a trusted publisher:

| Field       | Value                       |
|-------------|-----------------------------|
| Owner       | `abhishekbhakat`            |
| Repository  | `apache-airflow-dagsyncer`  |
| Workflow    | `release.yml`               |
| Environment | (leave blank)               |
