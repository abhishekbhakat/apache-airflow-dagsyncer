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

`apache-airflow-dagsyncer` is a command-line utility to sync Dag files from a
source location into an [Apache Airflow](https://airflow.apache.org/) dags
folder. It copies new and changed ``.py`` files while preserving the relative
directory layout, making it easy to promote Dags from a source repository into
a running Airflow deployment.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
**Table of contents**

- [Requirements](#requirements)
- [Installing from PyPI](#installing-from-pypi)
- [Getting started](#getting-started)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Requirements

| Component      | Version         |
|----------------|-----------------|
| Python         | 3.12+           |
| Apache Airflow | 3.2.2 or newer  |

## Installing from PyPI

```bash
pip install apache-airflow-dagsyncer
```

Or with [uv](https://docs.astral.sh/uv/):

```bash
uv pip install apache-airflow-dagsyncer
```

## Getting started

Sync Dags from a source directory into your Airflow dags folder:

```bash
apache-airflow-dagsyncer sync --source /path/to/dag-repo --dest $AIRFLOW_HOME/dags
```

Preview changes without copying anything:

```bash
apache-airflow-dagsyncer sync --source /path/to/dag-repo --dest $AIRFLOW_HOME/dags --dry-run
```

## Usage

```text
usage: apache-airflow-dagsyncer [-h] {sync} ...

Sync DAG files from a source location into an Airflow dags folder.

positional arguments:
  {sync}
    sync      Sync DAGs from source to destination

options:
  -h, --help  show this help message and exit
```

The `sync` command accepts:

| Option      | Description                                    |
|-------------|------------------------------------------------|
| `--source`  | Source directory containing Dag files          |
| `--dest`    | Destination Airflow dags folder                |
| `--dry-run` | Show what would be synced without copying      |

It can also be used programmatically:

```python
from airflow.dagsyncer.syncer import sync_dags

sync_dags(source="/path/to/dag-repo", dest="/opt/airflow/dags", dry_run=True)
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
