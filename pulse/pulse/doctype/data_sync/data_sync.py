# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import os
import time
from contextlib import suppress

import frappe
import ibis
import pandas as pd
from frappe.model.document import Document
from frappe.utils import get_files_path, get_table_name
from frappe.utils.file_lock import LockTimeoutError
from frappe.utils.synchronization import filelock
from ibis.backends.duckdb import Backend as DuckDBBackend

from pulse.logger import get_logger
from pulse.utils import get_etl_batch

_logger = get_logger()


class DataSync(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		batch_size: DF.Int
		checkpoint: DF.Data | None
		creation_key: DF.Data | None
		ended_at: DF.Datetime | None
		log: DF.LongText | None
		name: DF.Int | None
		primary_key: DF.Data | None
		reference_doctype: DF.Link | None
		started_at: DF.Datetime | None
		status: DF.Literal["Draft", "Queued", "Skipped", "In Progress", "Completed", "Failed"]
		table_name: DF.Data | None
		total_inserted: DF.Int
	# end: auto-generated types

	def set_value(self, fieldname, value, commit=True):
		self.set(fieldname, value)
		frappe.db.set_value(self.doctype, self.name, fieldname, value)
		self.notify_update()
		if commit:
			frappe.db.commit()

	def validate(self):
		meta = frappe.get_meta(self.reference_doctype)
		if meta.issingle:
			frappe.throw("Reference Doctype cannot be a Single Document")

	def log_msg(self, msg: str):
		if not self.log:
			self.log = ""
		self.log += f"{frappe.utils.now_datetime()}: {msg}\n"
		self.set_value("log", self.log)

	def before_save(self):
		self.creation_key = self.creation_key or "creation"
		self.primary_key = self.primary_key or "name"
		self.total_inserted = 0
		self.batch_size = 1000
		self.table_name = get_table_name(self.reference_doctype)
		self.calculate_batch_size()

	def should_sync(self):
		if self.status in ("Completed"):
			frappe.msgprint(f"Sync already {self.status.lower()}, skipping.")
			return False

		if not get_etl_batch(self.reference_doctype, batch_size=1):
			self.set_value("status", "Skipped")
			self.log_msg(f"No records found in {self.reference_doctype}, skipping sync.")
			return False

		return True

	@frappe.whitelist()
	def sync(self):
		if not self.should_sync():
			return

		self.set_value("log", None)
		self.set_value("started_at", frappe.utils.now_datetime())
		self.set_value("status", "In Progress")
		self.connect_to_warehouse()
		self.ensure_warehouse_table()
		self.fetch_checkpoint()

		lock_name = f"duckdb_sync:{self.table_name}"
		lock_timeout = 60

		try:
			with filelock(lock_name, timeout=lock_timeout):
				while True:
					batch = get_etl_batch(
						self.reference_doctype,
						checkpoint=self.checkpoint,
						batch_size=self.batch_size,
					)
					if not batch:
						self.log_msg(f"No new data to insert after {self.checkpoint}")
						break

					df = pd.DataFrame.from_records(batch)
					self.insert_batch(df)
					if df.shape[0] < self.batch_size:
						break
					time.sleep(0.01)
				self.set_value("ended_at", frappe.utils.now_datetime())
				self.set_value("status", "Completed")

		except LockTimeoutError:
			self.set_value("status", "Skipped")
			self.log_msg(
				f"Failed to acquire lock for {self.reference_doctype}, another sync already running."
			)

		except Exception as e:
			_logger.error(f"Error occurred while acquiring lock for {self.reference_doctype}: {e}")
			self.set_value("status", "Failed")
			self.log_msg(f"Error occurred: {e}")

	def connect_to_warehouse(self):
		self.warehouse_conn = get_warehouse_connection()

	def ensure_warehouse_table(self):
		doctype_schema = self.get_schema_from_meta()
		if not self.warehouse_conn.list_tables(like=self.table_name):
			self.warehouse_conn.create_table(self.table_name, schema=doctype_schema)
			self.log_msg(f"Created table {self.table_name} in warehouse.")
			return

		# t = self.warehouse_conn.table(self.table_name)
		# warehouse_schema = t.schema()
		# if not warehouse_schema.equals(doctype_schema):
		# 	self.log_msg(
		# 		f"Schema mismatch for table {self.table_name}.\n"
		# 		f"Warehouse schema: {warehouse_schema}\n"
		# 		f"Doctype schema: {doctype_schema}"
		# 	)
		# 	# TODO: alter the table
		# 	frappe.throw("Schema mismatch")

	def get_schema_from_meta(self):
		rows = get_etl_batch(self.reference_doctype, batch_size=1)
		df = pd.DataFrame.from_records(rows).fillna("")
		return ibis.memtable(df).schema()

	def calculate_batch_size(self, default_mb: int = 256) -> int:
		sample = get_etl_batch(self.reference_doctype, batch_size=10)
		df = pd.DataFrame.from_records(sample)
		if df.empty:
			return

		total_size = sum(df[col].memory_usage(deep=True) for col in df.columns)
		row_size_mb = (total_size / len(df)) / (1024 * 1024)
		if row_size_mb <= 0:
			return

		self.batch_size = int(default_mb / row_size_mb) or 1000
		self.set_value("batch_size", self.batch_size)

	def fetch_checkpoint(self):
		checkpoint = None
		with suppress(frappe.DoesNotExistError):
			last_sync = frappe.get_last_doc(
				"Data Sync",
				{
					"reference_doctype": self.reference_doctype,
					"status": "Completed",
				},
				order_by="ended_at desc",
			)
			checkpoint = last_sync.checkpoint
		self.set_value("checkpoint", checkpoint)

	def insert_batch(self, df) -> int:
		source = ibis.memtable(df)
		target = self.warehouse_conn.table(self.table_name)

		pred = [source[self.primary_key] == target[self.primary_key]]
		diff = source.anti_join(target, pred)
		diff = diff.select(target.columns)

		batch_count = df.shape[0]
		insert_count = int(diff.count().execute())
		skipped_count = batch_count - insert_count
		if insert_count > 0:
			self.warehouse_conn.insert(self.table_name, diff)

		self.checkpoint = df[self.creation_key].max()
		self.set_value("checkpoint", self.checkpoint)

		self.log_msg(
			f"Inserted {insert_count} rows up to {self.checkpoint}"
			+ (f" (Skipped: {skipped_count})" if skipped_count > 0 else "")
		)

		self.total_inserted += insert_count
		self.set_value("total_inserted", self.total_inserted)


def get_warehouse_connection() -> DuckDBBackend:
	"""Get or create a DuckDB connection to the warehouse database."""
	base = os.path.realpath(get_files_path(is_private=1))
	folder = os.path.join(base, "duckdb")
	os.makedirs(folder, exist_ok=True)
	db_path = os.path.join(folder, "warehouse.ducklake")

	conn: DuckDBBackend = ibis.duckdb.connect()
	conn.raw_sql("INSTALL ducklake;")
	conn.attach(f"ducklake:{db_path}", "warehouse")
	conn.raw_sql("USE warehouse;")
	return conn


def sync_doctype_to_duckdb(doctype: str):
	if not is_due(doctype):
		return

	doc = frappe.new_doc("Data Sync")
	doc.reference_doctype = doctype
	doc.save()
	doc.sync()


def is_due(doctype: str) -> bool:
	# TODO: store sync frequency per doctype, and calculate if now > last_sync + sync_frequency
	return True
