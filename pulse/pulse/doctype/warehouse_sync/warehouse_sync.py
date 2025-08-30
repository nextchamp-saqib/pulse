# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt


import frappe
import ibis
import pandas as pd
from frappe.model.document import Document
from frappe.utils import get_table_name

from pulse.logger import get_logger
from pulse.utils import get_etl_batch, get_warehouse_connection


class WarehouseSync(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		checkpoint: DF.Data | None
		creation_key: DF.Data
		enabled: DF.Check
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
		conn = get_warehouse_connection(readonly=False)
		doctype_schema = self.get_schema_from_meta()
		if not conn.list_tables(like=self.table_name):
			conn.create_table(self.table_name, schema=doctype_schema)
			return True
		return False

	def should_sync(self) -> bool:
		if not self.enabled:
			return False

		new_rows = get_etl_batch(self.reference_doctype, self.checkpoint, batch_size=1)
		if not new_rows:
			return False

		return True

	@frappe.whitelist()
	def start_sync(self):
		if not self.should_sync():
			get_logger().info("No new rows to sync or sync disabled")
			return

		job = frappe.new_doc("Warehouse Sync Job")
		job.config = self.name
		job.insert(ignore_permissions=True)
		job.run()
