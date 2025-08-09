import json
import os

import duckdb
import frappe

from .logger import get_logger

logger = get_logger()


def _get_db():
	db_dir = frappe.utils.get_site_path("private", "files")
	db_path = os.path.join(db_dir, "pulse.duckdb")
	conn = duckdb.connect(db_path)
	return conn


def _create_table(conn):
	conn.execute(
		"""
			CREATE TABLE IF NOT EXISTS events (
					id TEXT,
					site_name TEXT,
					event_name TEXT,
					app_name TEXT,
					app_version TEXT,
					timestamp TIMESTAMP,
					additional_data JSON
			)
		"""
	)


def store_batch_in_duckdb(log_data_list):
	conn = _get_db()
	try:
		_create_table(conn)

		rows = [
			(
				data["id"],
				data["site_name"],
				data["event_name"],
				data["app_name"],
				data["app_version"],
				data["timestamp"],
				json.dumps(data["additional_data"]),
			)
			for data in log_data_list
		]

		if not rows:
			return

		# Single-statement bulk insert with explicit transaction for atomicity
		conn.execute("BEGIN")
		placeholders = ", ".join(["(?, ?, ?, ?, ?, ?, ?)"] * len(rows))
		flat_params = [item for row in rows for item in row]
		conn.execute(
			f"""
				INSERT INTO events (id, site_name, event_name, app_name, app_version, timestamp, additional_data)
				VALUES {placeholders}
			""",
			flat_params,
		)
		conn.execute("COMMIT")
	except duckdb.Error as e:
		# Attempt rollback on failure
		try:
			conn.execute("ROLLBACK")
		except Exception:
			pass
		logger.error(f"Failed to store event batch in DuckDB: {e!s}")
		raise
	finally:
		conn.close()
