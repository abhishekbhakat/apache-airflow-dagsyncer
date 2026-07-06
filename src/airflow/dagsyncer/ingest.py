"""
Control-plane ingestion of manifests via Airflow's own update path.

Requires apache-airflow (the ``[listen]`` extra). Import errors here must not
break syncer-only installs, so this module is imported lazily by the server.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from airflow.dag_processing.collection import update_dag_parsing_results_in_db
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.utils.session import create_session
from sqlalchemy import select, update

from airflow import __version__ as airflow_version

if TYPE_CHECKING:
    from airflow.dagsyncer.protocol import Manifest

log = logging.getLogger(__name__)


class VersionSkewError(RuntimeError):
    """Raised when the manifest's Airflow version differs from the control plane's."""


def check_version_skew(manifest: Manifest) -> None:
    """Reject manifests produced by a different Airflow version."""
    if manifest.airflow_version != airflow_version:
        raise VersionSkewError(
            f"Manifest built with Airflow {manifest.airflow_version}, control plane runs {airflow_version}"
        )


def ingest_manifest(manifest: Manifest) -> dict[str, Any]:
    """Ingest a manifest into the control-plane DB; return summary counts."""
    check_version_skew(manifest)

    dags = [LazyDeserializedDAG(data=entry.data) for entry in manifest.dags]
    import_errors = {
        (manifest.bundle_name, error.filename): error.stacktrace for error in manifest.import_errors
    }
    manifest_dag_ids = {entry.dag_id for entry in manifest.dags}

    with create_session() as session:
        if session.get(DagBundleModel, manifest.bundle_name) is None:
            session.add(DagBundleModel(name=manifest.bundle_name))
            session.flush()

        update_dag_parsing_results_in_db(
            bundle_name=manifest.bundle_name,
            bundle_version=manifest.bundle_version,
            dags=dags,
            import_errors=import_errors,
            parse_duration=None,
            warnings=set(),
            session=session,
        )

        stale_query = select(DagModel.dag_id).where(
            DagModel.bundle_name == manifest.bundle_name,
            ~DagModel.is_stale,
        )
        if manifest_dag_ids:
            stale_query = stale_query.where(DagModel.dag_id.not_in(manifest_dag_ids))
        stale_ids = session.scalars(stale_query).all()
        if stale_ids:
            session.execute(update(DagModel).where(DagModel.dag_id.in_(stale_ids)).values(is_stale=True))

    log.info(
        "Ingested bundle %s version %s: %d dags upserted, %d deactivated, %d import errors",
        manifest.bundle_name,
        manifest.bundle_version,
        len(dags),
        len(stale_ids),
        len(import_errors),
    )
    return {
        "bundle_name": manifest.bundle_name,
        "bundle_version": manifest.bundle_version,
        "dags_upserted": len(dags),
        "dags_deactivated": len(stale_ids),
        "import_errors": len(import_errors),
    }
