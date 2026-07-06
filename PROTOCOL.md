# Syncer <-> Listener wire protocol (v1)

The syncer runs on the data plane next to the DagProcessor. The listener is a
FastAPI sub-app mounted into the control-plane Airflow api-server via the
plugin mechanism (`AirflowPlugin.fastapi_apps`, edge3 provider pattern), so it
inherits the api-server's port and TLS termination. All communication is
HTTP/S initiated by the syncer (edge-executor style: data plane dials out,
control plane never dials in).

## Flow

1. Deployment lands a new dag bundle on the data plane.
2. Syncer runs the DagProcessor once against a local SQLite metadata DB.
3. Syncer extracts serialized dag JSON + import errors into a Manifest.
4. Syncer POSTs the manifest to the listener.
5. Listener authenticates, validates versions, and ingests via Airflow's own
   update path (SerializedDagModel.write_dag / DagModel sync). Dags missing
   from the manifest are deactivated for that bundle.

## Endpoints

Served by the Airflow api-server under the `/dagsyncer` prefix.

| Method | Path                    | Purpose                       |
|--------|-------------------------|-------------------------------|
| GET    | `/dagsyncer/v1/health`  | Liveness + protocol version   |
| POST   | `/dagsyncer/v1/manifest`| Ingest a full bundle manifest |

## Authentication

`Authorization: Bearer <token>` with a shared token (v1). The control plane
configures the token via `[dagsyncer] api_secret_key` in Airflow config (or
`AIRFLOW__DAGSYNCER__API_SECRET_KEY`). Requests without a valid token get
`401`; an unconfigured secret yields `503`. JWT (edge-style) planned later.
TLS is the api-server's responsibility.

## Manifest payload

```json
{
  "protocol_version": 1,
  "airflow_version": "3.2.0",
  "bundle_name": "my-dags",
  "bundle_version": "2026-07-06T07:00:00Z-abc123",
  "dags": [
    {
      "dag_id": "example",
      "fileloc": "/dags/example.py",
      "dag_hash": "…",
      "data": { "…": "serialized dag JSON as produced by DagProcessor" }
    }
  ],
  "import_errors": [
    { "filename": "/dags/broken.py", "stacktrace": "…" }
  ]
}
```

The manifest is a **full snapshot per bundle**, not a delta. This makes
ingestion idempotent: replaying the same manifest is a no-op, and dag
removal is expressed by absence.

## Responses

| Status | Meaning                                                        |
|--------|----------------------------------------------------------------|
| 200    | Manifest ingested (body: counts of upserted/deactivated dags)  |
| 400    | Malformed manifest (schema/protocol_version mismatch)          |
| 401    | Missing or invalid auth token                                  |
| 409    | Airflow version skew between syncer and control plane          |
| 503    | Control plane DB unavailable; syncer should retry with backoff |

## Version rules

- `protocol_version` must match exactly; otherwise `400`.
- `airflow_version` must match the control plane's Airflow version
  (major.minor.patch); otherwise `409`. Serialized dag format is tied to
  the Airflow version, so no skew is tolerated in v1.
