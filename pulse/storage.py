import json
import os
from contextlib import contextmanager

import duckdb
import frappe

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
					id TEXT NOT NULL,
					site TEXT NOT NULL,
					name TEXT NOT NULL,
					created_at TIMESTAMP NOT NULL,
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

		rows = [
			(
				r["id"],
				r["site"],
				r["name"],
				r["app"],
				r["app_version"],
				r["frappe_version"],
				r["created_at"],
				json.dumps(r.get("data", {})),
			)
			for r in batch
		]

		if not rows:
			return

		insert_sql = """
			INSERT INTO event (id, site, name, app, app_version, frappe_version, created_at, data)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		"""

		with _transaction(conn):
			conn.executemany(insert_sql, rows)
	except duckdb.Error as e:
		logger.error(f"Failed to store event batch in DuckDB: {e!s}")
		raise
	finally:
		conn.close()
