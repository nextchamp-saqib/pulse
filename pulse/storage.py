import json
import os
from contextlib import contextmanager

import duckdb
import frappe
import pandas as pd

from .logger import get_logger

logger = get_logger()


def _get_db_path():
	return frappe.utils.get_site_path("private", "files", "pulse.duckdb")


def _get_db(read_only=True, **kwargs):
	return duckdb.connect(_get_db_path(), read_only=read_only, **kwargs)


def get_db_size() -> int:
	try:
		path = _get_db_path()
		return int(os.path.getsize(path))
	except Exception:
		return 0


def _ensure_table(conn):
	conn.execute(
		"""
			CREATE TABLE IF NOT EXISTS event (
					id TEXT,
					site TEXT,
					name TEXT,
					timestamp TIMESTAMP,
					app TEXT,
					app_version TEXT,
					frappe_version TEXT,
					data JSON,
					stored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			)
		"""
	)


@contextmanager
def _transaction(conn):
	"""Context manager to wrap a DuckDB transaction with automatic rollback on error."""
	conn.execute("BEGIN")
	try:
		yield
		conn.execute("COMMIT")
	except Exception:
		try:
			conn.execute("ROLLBACK")
		except Exception:
			pass
		raise


def store_batch_in_duckdb(batch):
	conn = _get_db(read_only=False)
	try:
		_ensure_table(conn)

		if not batch:
			return

		df = pd.DataFrame(  # noqa: F841
			[
				{
					"id": r.get("id"),
					"site": r.get("site"),
					"name": r.get("name"),
					"timestamp": r.get("timestamp"),
					"app": r.get("app"),
					"app_version": r.get("app_version"),
					"frappe_version": r.get("frappe_version"),
					"data": json.dumps(r.get("data", {})),
				}
				for r in batch
			]
		)

		with _transaction(conn):
			conn.execute("INSERT INTO event BY NAME SELECT * FROM df")
	except duckdb.Error as e:
		logger.error(f"Failed to store event batch in DuckDB: {e!s}")
		raise
	finally:
		conn.close()
