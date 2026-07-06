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

# Changelog

## 0.0.1rc2 (unreleased)

- Replace file-copy sync with syncer-listener architecture (edge-executor
  style: data plane pushes, control plane never dials in)
- Wire protocol v1 (`PROTOCOL.md`): full manifest snapshot per bundle,
  bearer auth, protocol and Airflow version checks
- `push` command: one-shot DagProcessor parse against local SQLite,
  manifest extraction, HTTP/S upload (stdlib only)
- `listen` command: stdlib HTTP server, constant-time token auth,
  ingestion via Airflow's `update_dag_parsing_results_in_db()`,
  deactivation of dags absent from the manifest (`[listen]` extra)
- Zero runtime dependencies for the core package; `apache-airflow>=3.2.0`
  behind the `[listen]` extra
- Apache 2.0 LICENSE, ruff + strict mypy, pytest suite, CI and tag-driven
  trusted-publishing release workflow

## 0.0.1rc1 (2026-07-06)

- Initial scaffold: namespace package `airflow.dagsyncer`, file-copy sync
  CLI (removed in rc2)
