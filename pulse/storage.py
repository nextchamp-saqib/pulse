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
					event_id TEXT NOT NULL,
					site_name TEXT NOT NULL,
					event_name TEXT NOT NULL,
					app_name TEXT NOT NULL,
					app_version TEXT,
					timestamp TIMESTAMP NOT NULL,
					additional_data JSON,
					created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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


def store_batch_in_duckdb(log_data_list):
	conn = _get_db(read_only=False)
	try:
		_ensure_table(conn)

		rows = [
			(
				data["event_id"],
				data["site_name"],
				data["event_name"],
				data["app_name"],
				data["app_version"],
				data["timestamp"],
				json.dumps(data.get("additional_data", {})),
			)
			for data in log_data_list
		]

		if not rows:
			return

		insert_sql = """
			INSERT INTO event (event_id, site_name, event_name, app_name, app_version, timestamp, additional_data)
			VALUES (?, ?, ?, ?, ?, ?, ?)
		"""

		with _transaction(conn):
			conn.executemany(insert_sql, rows)
	except duckdb.Error as e:
		logger.error(f"Failed to store event batch in DuckDB: {e!s}")
		raise
	finally:
		conn.close()
