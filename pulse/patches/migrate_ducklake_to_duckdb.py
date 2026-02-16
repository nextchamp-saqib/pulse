import os

import duckdb
import frappe
from frappe.utils import get_files_path

from pulse.logger import get_logger

logger = get_logger()

TABLE_NAME = "tabPulse Event"
WAREHOUSE_FILE = "warehouse.duckdb"
TEMP_FILE = "warehouse.duckdb.migrating"
BACKUP_FILE = "warehouse.ducklake.bak"


def execute(dry_run=False):
	paths = _get_paths()
	active_path = paths["active"]
	temp_path = paths["temp"]
	backup_path = paths["backup"]
	has_ducklake_table = _has_ducklake_table(active_path)
	is_duckdb = _is_duckdb_database(active_path)
	has_plain_duckdb_table = _has_plain_duckdb_table(active_path)

	logger.info(
		"Warehouse precheck: "
		f"dry_run={dry_run}, "
		f"active_exists={os.path.exists(active_path)}, "
		f"is_duckdb={is_duckdb}, "
		f"has_ducklake_table={has_ducklake_table}, "
		f"has_plain_duckdb_table={has_plain_duckdb_table}, "
		f"backup_exists={os.path.exists(backup_path)}"
	)

	if os.path.exists(backup_path) and is_duckdb and not has_ducklake_table:
		logger.info("Warehouse already migrated to DuckDB. Skipping patch.")
		return

	if not os.path.exists(active_path):
		if dry_run:
			logger.info("Dry run: no existing warehouse found. Would create empty DuckDB warehouse.")
			return

		_create_empty_duckdb(active_path)
		logger.info("No existing warehouse found. Created empty DuckDB warehouse.")
		return

	if has_ducklake_table:
		if os.path.exists(temp_path):
			os.remove(temp_path)

		migrate_pulse_event_table(active_path, temp_path)
		validate_migration(active_path, temp_path)

		if dry_run:
			if os.path.exists(temp_path):
				os.remove(temp_path)
			logger.info("Dry run: migration + validation succeeded. Skipped atomic file swap.")
			return

		_atomic_replace(active_path, temp_path, backup_path)
		logger.info("Warehouse migration completed. Runtime now uses plain DuckDB warehouse.duckdb")
		return

	if is_duckdb and has_plain_duckdb_table:
		logger.info("Warehouse already in DuckDB format. Skipping patch.")
		return

	if is_duckdb and not has_plain_duckdb_table and not has_ducklake_table:
		frappe.throw(
			"Warehouse is a DuckDB file, but source table was not found as DuckLake or plain DuckDB. "
			"Aborting to avoid false skip."
		)

	frappe.throw("Unsupported Warehouse file format. Migration aborted.")


def _get_paths() -> dict:
	base = os.path.realpath(get_files_path(is_private=1))
	return {
		"active": os.path.join(base, WAREHOUSE_FILE),
		"temp": os.path.join(base, TEMP_FILE),
		"backup": os.path.join(base, BACKUP_FILE),
	}


def _create_empty_duckdb(path: str):
	conn = duckdb.connect(path)
	conn.close()


def _is_duckdb_database(path: str) -> bool:
	if not os.path.exists(path):
		return False

	try:
		conn = duckdb.connect(path, read_only=True)
		conn.close()
		return True
	except Exception:
		return False


def _has_ducklake_table(path: str) -> bool:
	if not os.path.exists(path):
		return False

	conn = duckdb.connect()
	try:
		conn.execute("INSTALL ducklake;")
		conn.execute("LOAD ducklake;")
		conn.execute(f"ATTACH 'ducklake:{path}' AS old_warehouse (READ_ONLY);")
		return _table_exists(conn, "old_warehouse", TABLE_NAME)
	except Exception as e:
		frappe.throw(
			"Failed to inspect DuckLake source during migration precheck. "
			"Patch is aborting to avoid false skip. "
			f"Error: {e}"
		)
	finally:
		conn.close()


def _has_plain_duckdb_table(path: str) -> bool:
	if not os.path.exists(path):
		return False

	try:
		conn = duckdb.connect(path, read_only=True)
		try:
			return _table_exists(conn, "main", TABLE_NAME)
		finally:
			conn.close()
	except Exception:
		return False


def _atomic_replace(active_path: str, temp_path: str, backup_path: str):
	if os.path.exists(backup_path):
		os.remove(backup_path)

	os.replace(active_path, backup_path)
	os.replace(temp_path, active_path)


def migrate_pulse_event_table(old_db_path: str, new_db_path: str):
	conn = duckdb.connect()
	try:
		conn.execute("INSTALL ducklake;")
		conn.execute("LOAD ducklake;")
		conn.execute(f"ATTACH 'ducklake:{old_db_path}' AS old_warehouse (READ_ONLY);")
		conn.execute(f"ATTACH '{new_db_path}' AS new_warehouse;")

		if not _table_exists(conn, "old_warehouse", TABLE_NAME):
			logger.info(f"Source table {TABLE_NAME} not found in DuckLake. Nothing to backfill.")
			return

		conn.execute(
			f"""
			CREATE TABLE IF NOT EXISTS new_warehouse.\"{TABLE_NAME}\" AS
			SELECT * FROM old_warehouse.\"{TABLE_NAME}\" LIMIT 0
			"""
		)

		conn.execute(
			f"""
			INSERT INTO new_warehouse.\"{TABLE_NAME}\"
			SELECT * FROM old_warehouse.\"{TABLE_NAME}\"
			"""
		)
	finally:
		conn.close()


def validate_migration(old_db_path: str, new_db_path: str):
	conn = duckdb.connect()
	try:
		conn.execute("INSTALL ducklake;")
		conn.execute("LOAD ducklake;")
		conn.execute(f"ATTACH 'ducklake:{old_db_path}' AS old_warehouse (READ_ONLY);")
		conn.execute(f"ATTACH '{new_db_path}' AS new_warehouse (READ_ONLY);")

		if not _table_exists(conn, "old_warehouse", TABLE_NAME):
			return
		if not _table_exists(conn, "new_warehouse", TABLE_NAME):
			frappe.throw(f"Migration validation failed: destination table {TABLE_NAME} is missing")

		old_count = conn.execute(
			f"SELECT COUNT(*) FROM old_warehouse.\"{TABLE_NAME}\""
		).fetchone()[0]
		new_count = conn.execute(
			f"SELECT COUNT(*) FROM new_warehouse.\"{TABLE_NAME}\""
		).fetchone()[0]

		old_max_name = conn.execute(
			f"SELECT MAX(name) FROM old_warehouse.\"{TABLE_NAME}\""
		).fetchone()[0]
		new_max_name = conn.execute(
			f"SELECT MAX(name) FROM new_warehouse.\"{TABLE_NAME}\""
		).fetchone()[0]

		if old_count != new_count or old_max_name != new_max_name:
			frappe.throw(
				"Migration validation failed: row count / max(name) mismatch "
				f"(old_count={old_count}, new_count={new_count}, "
				f"old_max_name={old_max_name}, new_max_name={new_max_name})"
			)
	finally:
		conn.close()


def _table_exists(conn, database_name: str, table_name: str) -> bool:
	try:
		conn.execute(f"SELECT 1 FROM {database_name}.\"{table_name}\" LIMIT 1")
		return True
	except Exception:
		return False
