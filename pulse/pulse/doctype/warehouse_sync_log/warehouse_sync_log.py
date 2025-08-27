# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import time

import frappe
import ibis
import pandas as pd
from frappe.model.document import Document
from frappe.utils.file_lock import LockTimeoutError
from frappe.utils.synchronization import filelock

from pulse.logger import get_logger
from pulse.pulse.doctype.warehouse_sync_job.warehouse_sync_job import (
	WarehouseSyncJob,
	get_warehouse_connection,
)
from pulse.utils import get_etl_batch

_logger = get_logger()


class WarehouseSyncLog(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		batch_size: DF.Int
		ended_at: DF.Datetime | None
		job: DF.Link
		log: DF.LongText | None
		name: DF.Int | None
		started_at: DF.Datetime | None
		status: DF.Literal["Queued", "In Progress", "Completed", "Failed", "Skipped"]
		total_inserted: DF.Int
	# end: auto-generated types

	def set_value(self, fieldname, value, commit=True):
		self.set(fieldname, value)
		frappe.db.set_value(self.doctype, self.name, fieldname, value)
		self.notify_update()
		if commit:
			frappe.db.commit()

	def log_msg(self, msg: str):
		if not self.log:
			self.log = ""
		self.log += f"{frappe.utils.now_datetime()}: {msg}\n"
		self.set_value("log", self.log)

	def before_insert(self):
		self.total_inserted = 0
		self.batch_size = self.batch_size or 1000

	def _load_job(self) -> WarehouseSyncJob:
		return frappe.get_doc("Warehouse Sync Job", self.job)

	@frappe.whitelist()
	def sync(self):
		job = self._load_job()
		if not job.enabled:
			self.set_value("status", "Skipped")
			self.log_msg("Job is disabled, skipping.")
			return

		# compute batch size using row_size if available (target ~256MB)
		if getattr(job, "row_size", None):
			self.batch_size = max(int((256 * 1024 * 1024) / max(job.row_size, 1)), 1)
			self.set_value("batch_size", self.batch_size)

		self.set_value("log", None)
		self.set_value("started_at", frappe.utils.now_datetime())
		self.set_value("status", "In Progress")

		# ensure warehouse table exists before starting
		created = job.ensure_warehouse_table()
		if created:
			self.log_msg(f"Created table {job.table_name} in warehouse.")

		checkpoint = job.checkpoint
		self.set_value("status", "In Progress")

		lock_name = f"duckdb_sync:{job.table_name}"
		lock_timeout = 60

		try:
			with filelock(lock_name, timeout=lock_timeout):
				conn = get_warehouse_connection()
				while True:
					batch = get_etl_batch(
						job.reference_doctype,
						checkpoint=checkpoint,
						batch_size=self.batch_size,
					)
					if not batch:
						self.log_msg(f"No new data to insert after {checkpoint}")
						break

					df = pd.DataFrame.from_records(batch)
					inserted, checkpoint = self._insert_batch(conn, job, df)
					if df.shape[0] < self.batch_size:
						break
					time.sleep(0.01)
			self.set_value("ended_at", frappe.utils.now_datetime())
			self.set_value("status", "Completed")

		except LockTimeoutError:
			self.set_value("status", "Skipped")
			self.log_msg(
				f"Failed to acquire lock for {job.reference_doctype}, another sync already running."
			)

		except Exception as e:
			_logger.error(
				f"Error occurred while synchronizing {job.reference_doctype} to warehouse: {e}"
			)
			self.set_value("status", "Failed")
			self.log_msg(f"Error occurred: {e}")

	def _insert_batch(self, conn, job, df) -> tuple[int, str | None]:
		"""Insert only new rows into warehouse and update counters.

		Returns (inserted_count, new_checkpoint)
		"""
		source = ibis.memtable(df)
		target = conn.table(job.table_name)

		pred = [source[job.primary_key] == target[job.primary_key]]
		diff = source.anti_join(target, pred)
		diff = diff.select(target.columns)

		batch_count = df.shape[0]
		insert_count = int(diff.count().execute())
		skipped_count = batch_count - insert_count
		if insert_count > 0:
			conn.insert(job.table_name, diff)

		checkpoint = df[job.creation_key].max()
		# persist checkpoint on the job
		job.set_value("checkpoint", checkpoint)

		self.log_msg(
			f"Inserted {insert_count} rows up to {checkpoint}" + (
				f" (Skipped: {skipped_count})" if skipped_count > 0 else ""
			)
		)

		self.total_inserted = (self.total_inserted or 0) + insert_count
		self.set_value("total_inserted", self.total_inserted)
		return insert_count, checkpoint
