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

# Apache Airflow DagSyncer

| Category | Badges |
|----------|--------|
| License  | [![License](https://img.shields.io/:license-Apache%202-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt) |
| PyPI     | [![PyPI version](https://badge.fury.io/py/apache-airflow-dagsyncer.svg)](https://badge.fury.io/py/apache-airflow-dagsyncer) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/apache-airflow-dagsyncer.svg)](https://pypi.org/project/apache-airflow-dagsyncer/) |

`apache-airflow-dagsyncer` ships serialized dags from a data plane to an
[Apache Airflow](https://airflow.apache.org/) control plane over HTTP/S -
edge-executor style. The control plane never parses dag files and never
dials into the data plane.

```text
  Data plane (remote)                        Control plane
+------------------------+    HTTP/S      +------------------------+
| DagProcessor (run-once |  ---------->   | listener               |
|   against local SQLite)|   manifest     |   auth + validation    |
| syncer (push)          |                |   SerializedDagModel   |
|   extracts serialized  |                |     .write_dag()       |
|   dags, POSTs manifest |                |   -> control-plane DB  |
+------------------------+                +------------------------+
```

On each deployment the syncer runs the DagProcessor once against a local
SQLite database, extracts the serialized dag JSON, and pushes a full bundle
manifest to the listener. The listener ingests it through Airflow's own
update path, so versioning, hashing, and consistency stay Airflow's
responsibility. Dags absent from the manifest are deactivated.

See [PROTOCOL.md](https://github.com/abhishekbhakat/apache-airflow-dagsyncer/blob/main/PROTOCOL.md) for the wire protocol.

**Status: early development.** The `push` and `listen` commands are not
implemented yet; the wire protocol (v1) is defined.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
**Table of contents**

- [Requirements](#requirements)
- [Installing from PyPI](#installing-from-pypi)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Requirements

| Component | Version |
|-----------|---------|
| Python    | 3.12+   |

The syncer side has no runtime dependencies. The listener side requires
Apache Airflow, installed via the `[listen]` extra:

```bash
pip install "apache-airflow-dagsyncer[listen]"
```

## Installing from PyPI

```bash
pip install apache-airflow-dagsyncer
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install apache-airflow-dagsyncer
```

## Usage

Data plane - parse a bundle and push it to the control plane:

```bash
apache-airflow-dagsyncer push \
  --bundle-path /opt/dags \
  --bundle-name my-dags \
  --listener-url https://cp.example.com:8793 \
  --token "$DAGSYNCER_TOKEN"
```

Control plane - run the listener:

```bash
apache-airflow-dagsyncer listen --host 0.0.0.0 --port 8793
```

## Contributing

Want to help build this project? Set up a development environment with
[uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/abhishekbhakat/apache-airflow-dagsyncer.git
cd apache-airflow-dagsyncer
uv sync
uv run apache-airflow-dagsyncer --help
```

## License

[Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0)
