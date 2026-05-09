"""
Rebuild the Pulse Event warehouse table with:
  - TIMESTAMP columns for captured_at / received_at  (was VARCHAR)
  - Rows written ORDER BY event_name, site, app, user, received_at for compression
  - creation and modified columns dropped  (they are always == received_at)

Measured savings on a 2.8 M-row dataset:
  VARCHAR + unsorted  → ~248 MiB  (baseline)
  TIMESTAMP + sorted  → ~101 MiB  (~59 % smaller)
  + drop creation/modified → ~73 MiB  (~71 % smaller)

The patch uses the same atomic-replace pattern as migrate_ducklake_to_duckdb:
  1. write a temp file, 2. validate, 3. os.replace into place.
"""

import os

import duckdb
import frappe
from frappe.utils import get_files_path

from pulse.logger import get_logger

logger = get_logger()

TABLE_NAME = "tabPulse Event"
WAREHOUSE_FILE = "warehouse.duckdb"
TEMP_FILE = "warehouse.duckdb.optimizing"
BACKUP_FILE = "warehouse.duckdb.pre-optimize.bak"


def execute():
	paths = _get_paths()
	active_path = paths["active"]
	temp_path = paths["temp"]
	backup_path = paths["backup"]

	if not os.path.exists(active_path):
		logger.info("No existing warehouse found. Nothing to optimize.")
		return

	if not _table_exists_in_file(active_path):
		logger.info(f"Table {TABLE_NAME!r} not found in warehouse. Nothing to optimize.")
		return

	if _is_already_optimized(active_path):
		logger.info("Warehouse already has typed TIMESTAMP columns. Skipping rebuild.")
		_ensure_sort_by_configured()
		return

	if os.path.exists(temp_path):
		os.remove(temp_path)

	logger.info("Rebuilding warehouse with TIMESTAMP columns and optimal sort order...")
	_rebuild_optimized(active_path, temp_path)
	_validate(active_path, temp_path)
	_atomic_replace(active_path, temp_path, backup_path)

	_ensure_sort_by_configured()
	logger.info("Warehouse optimization complete.")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _get_paths() -> dict:
	base = os.path.realpath(get_files_path(is_private=1))
	return {
		"active": os.path.join(base, WAREHOUSE_FILE),
		"temp": os.path.join(base, TEMP_FILE),
		"backup": os.path.join(base, BACKUP_FILE),
	}


def _table_exists_in_file(path: str) -> bool:
	try:
		conn = duckdb.connect(path, read_only=True)
		try:
			conn.execute(f'SELECT 1 FROM "{TABLE_NAME}" LIMIT 1')
			return True
		except Exception:
			return False
		finally:
			conn.close()
	except Exception:
		return False


def _is_already_optimized(path: str) -> bool:
	"""Return True if received_at is already stored as TIMESTAMP (not VARCHAR)."""
	try:
		conn = duckdb.connect(path, read_only=True)
		try:
			row = conn.execute(
				"SELECT data_type FROM information_schema.columns "
				"WHERE table_name = ? AND column_name = 'received_at'",
				[TABLE_NAME],
			).fetchone()
			return bool(row and row[0].upper().startswith("TIMESTAMP"))
		finally:
			conn.close()
	except Exception:
		return False


def _rebuild_optimized(old_path: str, new_path: str):
	"""
	Create new_path from scratch with the optimized schema.

	Columns kept   : name, event_name, captured_at, properties, site, app, user, received_at
	Columns dropped: creation, modified  (always == received_at, pure redundancy)
	Type changes   : captured_at, received_at  VARCHAR → TIMESTAMP
	Row order      : event_name, site, app, user, received_at
	"""
	conn = duckdb.connect(new_path)
	try:
		conn.execute(f"ATTACH '{old_path}' AS old_wh (READ_ONLY)")
		conn.execute(
			f"""
			CREATE TABLE "{TABLE_NAME}" AS
			SELECT
				name,
				event_name,
				TRY_CAST(captured_at AS TIMESTAMP) AS captured_at,
				properties,
				site,
				app,
				"user",
				TRY_CAST(received_at AS TIMESTAMP) AS received_at
			FROM old_wh."{TABLE_NAME}"
			ORDER BY event_name, site, app, "user", TRY_CAST(received_at AS TIMESTAMP)
			"""
		)
	finally:
		conn.close()


def _validate(old_path: str, new_path: str):
	old_conn = duckdb.connect(old_path, read_only=True)
	new_conn = duckdb.connect(new_path, read_only=True)
	try:
		old_count = old_conn.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME}"').fetchone()[0]
		new_count = new_conn.execute(f'SELECT COUNT(*) FROM "{TABLE_NAME}"').fetchone()[0]
		if old_count != new_count:
			frappe.throw(
				f"Warehouse optimization validation failed: "
				f"source has {old_count} rows, rebuilt table has {new_count} rows."
			)

		old_max = old_conn.execute(f'SELECT MAX(name) FROM "{TABLE_NAME}"').fetchone()[0]
		new_max = new_conn.execute(f'SELECT MAX(name) FROM "{TABLE_NAME}"').fetchone()[0]
		if old_max != new_max:
			frappe.throw(
				f"Warehouse optimization validation failed: MAX(name) mismatch "
				f"(old={old_max}, new={new_max})."
			)
	finally:
		old_conn.close()
		new_conn.close()


def _atomic_replace(active_path: str, temp_path: str, backup_path: str):
	if os.path.exists(backup_path):
		os.remove(backup_path)
	os.replace(active_path, backup_path)
	os.replace(temp_path, active_path)


def _ensure_sort_by_configured():
	"""Set sort_by on the existing Warehouse Sync record if not already set."""
	if frappe.db.exists("Warehouse Sync", "Pulse Event"):
		current = frappe.db.get_value("Warehouse Sync", "Pulse Event", "sort_by")
		if not current:
			frappe.db.set_value(
				"Warehouse Sync",
				"Pulse Event",
				"sort_by",
				"event_name,site,app,user,received_at",
			)
			frappe.db.commit()
