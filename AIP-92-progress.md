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

# AIP-92 progress tracker

This file tracks the upstream Apache Airflow Improvement Proposal that
`apache-airflow-dagsyncer` is working around.

## What is AIP-92?

[AIP-92 "Isolate DAG processor, Callback processor, and Triggerer from core
services"](https://cwiki.apache.org/confluence/display/AIRFLOW/%5BWIP%5D+AIP-92+Isolate+DAG+processor%2C+Callback+processor%2C+and+Triggerer+from+core+services)
is a draft proposal to move DAG processing (and later callbacks / triggerers)
from direct metadata-DB access to API-server endpoints. The goal is the same
as this package: user code on the data plane should never need control-plane
DB credentials.

## Current status

| Date         | Milestone |
|--------------|-----------|
| 2025-07-24   | Sumit Maheshwari posted the [DISCUSS thread](https://lists.apache.org/thread/8vopwr8rydogf9ynfspsglqhw3lcblk5) to dev@airflow.apache.org. |
| 2025-07-25   | Jarek Potiuk, Ash Berlin-Taylor, Igor Kholopov reviewed. Consensus: concept supported, scope should include triggerer, auth must be per-bundle long-lived tokens, bundle-level API is the right unit. |
| 2025-07-31   | Sumit expanded the proposal into three phases (DAG processor, callback processor, triggerer) and asked for further review. |
| 2025-08-13   | Last update to the Confluence page. Status remains **Draft**; no recorded vote result. |
| 2026-04-28   | Airflow main: Ephraim Anierobi merged a seam to allow swapping callback fetching from DB to API (`1b8b0ff40e`). |
| 2026-05-07   | Airflow main: added `purge_inactive_dag_warnings()` and bundle-refresh override seams explicitly for AIP-92 subclasses (`34ef2503e1`). |
| 2026-05-??   | Airflow main: decoupled session ownership and removed `bundle_version` from parse-result helpers (`6761d3bc37`). |
| 2026-??-??   | Airflow 3.3.0 released. Features include multi-language Task SDK, task/asset state store, pluggable retries, "Dag as an API" (return-value surface, unrelated to DAG-processor isolation). **AIP-92 still not implemented.** dagsyncer remains necessary; CI matrixes floor 3.2.0 + current 3.3.0. |

## What this means for dagsyncer

Upstream AIP-92 is not yet implemented end-to-end, but the DAG processor is
being refactored to make the DB-to-API swap possible.

`apache-airflow-dagsyncer` is an **interim workaround**: it provides the same
outcome (data-plane DAG processing, control-plane DB isolation) by running a
one-shot DagProcessor on the data plane and shipping the serialized DAG
manifest to a listener plugin in the Airflow api-server. When AIP-92 lands
natively, this package should become unnecessary and can be deprecated.

## Decision log

- 2026-07-06: confirmed AIP-92 still Draft, no upstream fix available; keep
  dagsyncer as bridge solution.
