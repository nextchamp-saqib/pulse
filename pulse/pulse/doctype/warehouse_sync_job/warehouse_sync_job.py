# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import os

import frappe
import ibis
import pandas as pd
from frappe.model.document import Document
from frappe.utils import get_files_path, get_table_name
from ibis.backends.duckdb import Backend as DuckDBBackend

from pulse.utils import get_etl_batch


class WarehouseSyncJob(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		checkpoint: DF.Data | None
		creation_key: DF.Data
		enabled: DF.Check
		name: DF.Int | None
		primary_key: DF.Data
		reference_doctype: DF.Link
		row_size: DF.Int
		table_name: DF.Data | None
	# end: auto-generated types

	def set_value(self, fieldname, value, commit: bool = True):
		self.set(fieldname, value)
		frappe.db.set_value(self.doctype, self.name, fieldname, value)
		self.notify_update()
		if commit:
			frappe.db.commit()

	def validate(self):
		meta = frappe.get_meta(self.reference_doctype)
		if meta.issingle:
			frappe.throw("Reference Doctype cannot be a Single Document")

	def before_save(self):
		self.creation_key = self.creation_key or "creation"
		self.primary_key = self.primary_key or "name"
		self.table_name = get_table_name(self.reference_doctype)
		self.calculate_row_size()

	def calculate_row_size(self, sample_size: int = 10):
		"""
		Estimate average row size in bytes and persist in row_size.
		Uses a small sample from the source doctype to estimate memory usage.
		"""
		sample = get_etl_batch(self.reference_doctype, batch_size=sample_size)
		df = pd.DataFrame.from_records(sample)
		if df.empty:
			return
		total_size = sum(df[col].memory_usage(deep=True) for col in df.columns)
		row_size_bytes = int(total_size / max(len(df), 1))
		self.row_size = row_size_bytes

	def get_schema_from_meta(self):
		"""Derive an ibis schema from a sample of source records."""
		rows = get_etl_batch(self.reference_doctype, batch_size=1)
		df = pd.DataFrame.from_records(rows).fillna("")
		return ibis.memtable(df).schema()

	def ensure_warehouse_table(self) -> bool:
		"""Ensure the warehouse table exists. Returns True if created."""
		if not self.table_name:
			frappe.throw("Table name is not set on the Job")
		conn = get_warehouse_connection()
		doctype_schema = self.get_schema_from_meta()
		if not conn.list_tables(like=self.table_name):
			conn.create_table(self.table_name, schema=doctype_schema)
			return True
		return False

	@frappe.whitelist()
	def start_sync(self):
		"""Create a Warehouse Sync Log and execute the sync now."""
		log = frappe.new_doc("Warehouse Sync Log")
		log.job = self.name
		log.insert(ignore_permissions=True)
		log.sync()
		return log.name


def get_warehouse_connection() -> DuckDBBackend:
	"""Get or create a DuckDB connection to the warehouse database (ducklake)."""
	base = os.path.realpath(get_files_path(is_private=1))
	folder = os.path.join(base, "duckdb")
	os.makedirs(folder, exist_ok=True)
	db_path = os.path.join(folder, "warehouse.ducklake")

	conn: DuckDBBackend = ibis.duckdb.connect()
	# ensure ducklake extension is available and use attached DB
	conn.raw_sql("INSTALL ducklake;")
	conn.attach(f"ducklake:{db_path}", "warehouse")
	conn.raw_sql("USE warehouse;")
	return conn


def sync_warehouse():
	if not frappe.db.exists("Warehouse Sync Job", {"enabled": 1}):
		return

	for job in frappe.get_all("Warehouse Sync Job", filters={"enabled": 1}):
		log = frappe.new_doc("Warehouse Sync Log")
		log.job = job.name
		if log.should_sync():
			log.insert(ignore_permissions=True)
			log.sync()
