"""Drop the duckdb warehouse sync layer and move events into MariaDB.

`Pulse Event` is now a real doctype stored in the site database (Insights connects
to it directly and imports into its own duckdb). This patch:

  1. Backfills any events sitting in the old `warehouse.duckdb` into `tabPulse Event`.
  2. Removes the now-defunct `Warehouse Sync` / `Warehouse Sync Job` doctypes.

The duckdb backfill is best-effort: if duckdb is no longer installed (deps were
removed) or no warehouse file exists, we skip the backfill and still drop the
doctypes. Insights already holds a full load of historical events, so a missed
backfill is recoverable.
"""

import json
import os
from ast import literal_eval

import frappe
from frappe.utils import get_files_path, now_datetime

from pulse.logger import get_logger

logger = get_logger()

WAREHOUSE_FILE = "warehouse.duckdb"
TABLE_NAME = "tabPulse Event"
BATCH_SIZE = 5000


def execute():
	# Drop the defunct doctypes inline (cheap), but run the duckdb backfill in the
	# background: it can be millions of rows and would otherwise block the migrate
	# downtime window. New events already flow ingest -> Redis -> database the moment
	# this deploys, so the historical rows can load while the site serves traffic.
	# The backfill is idempotent (ignore_duplicates on the entry-id primary key), so
	# it is safe to retry if the job fails partway.
	_drop_warehouse_doctypes()
	frappe.enqueue(
		"pulse.patches.migrate_warehouse_to_db._backfill_from_duckdb",
		queue="long",
		timeout=3600,
		job_id="pulse_warehouse_backfill",
		deduplicate=True,
	)
	logger.info("Queued background backfill of historical events from warehouse.duckdb.")


def _warehouse_path():
	base = os.path.realpath(get_files_path(is_private=1))
	return os.path.join(base, WAREHOUSE_FILE)


def _coerce_properties(value):
	"""Return a valid JSON string for the properties column.

	Older rows stored `properties` as a Python-repr string (cstr of a dict), which
	is not valid JSON. Try JSON first, then literal_eval, then give up with `{}`.
	"""
	if value in (None, ""):
		return "{}"
	if isinstance(value, dict | list):
		return frappe.as_json(value)
	if isinstance(value, str):
		try:
			json.loads(value)
			return value
		except (ValueError, TypeError):
			try:
				return json.dumps(literal_eval(value))
			except (ValueError, SyntaxError):
				return "{}"
	return "{}"


def _backfill_from_duckdb():
	path = _warehouse_path()
	if not os.path.exists(path):
		logger.info("No warehouse.duckdb found; skipping event backfill.")
		return

	try:
		import duckdb
	except ImportError:
		logger.warning("duckdb not installed; skipping event backfill from warehouse.duckdb.")
		return

	fields = [
		"name",
		"event_name",
		"captured_at",
		"site",
		"app",
		"user",
		"team",
		"properties",
		"creation",
		"modified",
		"owner",
		"modified_by",
	]

	# Columns we care about; the duckdb table only contains whatever the old Ibis
	# schema inference picked up from sample rows, so select the intersection.
	wanted = ["name", "event_name", "captured_at", "received_at", "site", "app", "user", "team", "properties"]

	con = duckdb.connect(path, read_only=True)
	try:
		tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
		if TABLE_NAME not in tables:
			logger.info("warehouse.duckdb has no Pulse Event table; nothing to backfill.")
			return

		available = {r[0] for r in con.execute(f'DESCRIBE "{TABLE_NAME}"').fetchall()}
		select_cols = [c for c in wanted if c in available]
		if "name" not in select_cols:
			logger.warning("warehouse.duckdb Pulse Event table has no `name` column; skipping backfill.")
			return

		col_sql = ", ".join(f'"{c}"' for c in select_cols)
		cursor = con.execute(f'SELECT {col_sql} FROM "{TABLE_NAME}"')
		total = 0
		while True:
			chunk = cursor.fetchmany(BATCH_SIZE)
			if not chunk:
				break
			fallback_ts = now_datetime()
			rows = []
			for r in chunk:
				record = dict(zip(select_cols, r, strict=True))
				# Preserve when the event happened, not the migration time.
				audit_ts = record.get("received_at") or record.get("captured_at") or fallback_ts
				rows.append(
					(
						record.get("name"),
						record.get("event_name"),
						record.get("captured_at"),
						record.get("site"),
						record.get("app"),
						record.get("user"),
						record.get("team"),
						_coerce_properties(record.get("properties")),
						audit_ts,  # creation
						audit_ts,  # modified
						"Administrator",  # owner
						"Administrator",  # modified_by
					)
				)
			frappe.db.bulk_insert("Pulse Event", fields, rows, ignore_duplicates=True)
			frappe.db.commit()
			total += len(rows)

		logger.info(f"Backfilled {total} events from warehouse.duckdb into the database.")
	finally:
		con.close()


def _drop_warehouse_doctypes():
	for doctype in ("Warehouse Sync Job", "Warehouse Sync"):
		if frappe.db.exists("DocType", doctype):
			frappe.delete_doc("DocType", doctype, force=True, ignore_missing=True)
			logger.info(f"Removed defunct doctype: {doctype}")

	# Drop the File record that tracked the duckdb warehouse, if present.
	frappe.db.delete("File", {"file_url": f"/private/files/{WAREHOUSE_FILE}"})
	frappe.db.commit()
