# Syncer <-> Listener wire protocol (v1)

The syncer runs on the data plane next to the DagProcessor. The listener runs
on the control plane with DB access. All communication is HTTP/S initiated by
the syncer (edge-executor style: data plane dials out, control plane never
dials in).

## Flow

1. Deployment lands a new dag bundle on the data plane.
2. Syncer runs the DagProcessor once against a local SQLite metadata DB.
3. Syncer extracts serialized dag JSON + import errors into a Manifest.
4. Syncer POSTs the manifest to the listener.
5. Listener authenticates, validates versions, and ingests via Airflow's own
   update path (SerializedDagModel.write_dag / DagModel sync). Dags missing
   from the manifest are deactivated for that bundle.

## Endpoints

| Method | Path              | Purpose                          |
|--------|-------------------|----------------------------------|
| GET    | `/api/v1/health`  | Liveness + protocol version      |
| POST   | `/api/v1/manifest`| Ingest a full bundle manifest    |

## Authentication

`Authorization: Bearer <token>` with a shared token (v1). JWT (edge-style)
planned later. Requests without a valid token get `401`.

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
