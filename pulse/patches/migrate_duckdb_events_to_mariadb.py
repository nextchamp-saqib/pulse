"""
Migrate existing Pulse Events from warehouse.duckdb into MariaDB (tabPulse Event).

Uses ibis to read from DuckDB and frappe.model.document.bulk_insert for insertion.
The warehouse file is left untouched so it can serve as a backup.
"""

import os

import frappe
from frappe.model.document import bulk_insert
from frappe.utils import get_files_path

from pulse.logger import get_logger

logger = get_logger()

TABLE_NAME = "tabPulse Event"
BATCH_SIZE = 5000


def execute():
	db_path = _get_db_path()
	if not os.path.exists(db_path):
		logger.info("migrate_duckdb_events_to_mariadb: warehouse.duckdb not found, nothing to migrate.")
		return

	import ibis

	conn = ibis.duckdb.connect(db_path, read_only=True)
	try:
		if TABLE_NAME not in conn.list_tables():
			logger.info(
				f"migrate_duckdb_events_to_mariadb: table '{TABLE_NAME}' not found in warehouse, nothing to migrate."
			)
			return

		table = conn.table(TABLE_NAME)
		total = table.count().execute()
		if total == 0:
			logger.info("migrate_duckdb_events_to_mariadb: warehouse table is empty, nothing to migrate.")
			return

		logger.info(f"migrate_duckdb_events_to_mariadb: migrating {total} rows from DuckDB → MariaDB")

		COLS = ["name", "event_name", "captured_at", "received_at", "site", "app", "user", "properties"]
		existing_cols = [c for c in COLS if c in table.columns]
		df = table.select(existing_cols).order_by("captured_at").execute()

		bulk_insert(
			"Pulse Event",
			_doc_generator(df, existing_cols),
			chunk_size=BATCH_SIZE,
			commit_chunks=True,
		)

		logger.info(f"migrate_duckdb_events_to_mariadb: migration complete, {len(df)} rows inserted.")
	finally:
		conn.disconnect()


def _doc_generator(df, cols):
	for _, row in df.iterrows():
		doc = frappe.new_doc("Pulse Event")
		for col in cols:
			doc.set(col, row.get(col))
		yield doc


def _get_db_path():
	base = os.path.realpath(get_files_path(is_private=1))
	return os.path.join(base, "warehouse.duckdb")
