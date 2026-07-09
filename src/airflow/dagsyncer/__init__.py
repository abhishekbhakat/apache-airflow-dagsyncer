"""apache-airflow-dagsyncer package."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("apache-airflow-dagsyncer")
except PackageNotFoundError:
    # Source tree without an install (e.g. bare checkout).
    __version__ = "0.0.0"
