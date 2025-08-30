# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

from datetime import datetime

import frappe
from frappe.model.document import Document
from frappe.utils import now_datetime
from frappe.utils.logger import get_logger

from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream
from pulse.pulse.doctype.warehouse_sync_job.warehouse_sync_job import WarehouseSyncJob

logger = get_logger()


_EVENT_STREAM = None


def _get_event_stream():
	"""Return a cached Redis stream for pulse events (initialized once per process)."""
	global _EVENT_STREAM
	if _EVENT_STREAM is None:
		_EVENT_STREAM = RedisStream.init()
	return _EVENT_STREAM



REQD_FIELDS = ["event_name", "captured_at"]


class PulseEvent(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		app: DF.Data | None
		captured_at: DF.Datetime | None
		event_name: DF.Data | None
		properties: DF.JSON | None
		received_at: DF.Datetime | None
		site: DF.Data | None
		user: DF.Data | None
	# end: auto-generated types

	@property
	def stream(self):
		return _get_event_stream()

	def validate(self):
		missing = [field for field in REQD_FIELDS if not getattr(self, field)]

		if missing:
			frappe.throw(f"Missing required fields: {', '.join(missing)}")

	def db_insert(self, *args, **kwargs):
		self.validate()
		self.stream.add(
			{
				"event_name": self.get("event_name"),
				"captured_at": self.get("captured_at"),
				"site": self.get("site"),
				"user": self.get("user"),
				"app": self.get("app"),
				"properties": self.get("properties") or {},
				"received_at": now_datetime(),
			}
		)

	def load_from_db(self):
		entry = self.stream.get_entry(self.name)
		doc = PulseEvent._from_stream_entry(entry)
		super(Document, self).__init__(doc)

	@staticmethod
	def _from_stream_entry(entry):
		data = entry.get("data", {})
		# timestamp_ms = int(entry.get("id").split("-")[0])
		# timestamp_s = timestamp_ms / 1000
		# creation = datetime.fromtimestamp(timestamp_s)

		return {
			"name": entry.get("id"),
			"event_name": data.get("event_name"),
			"captured_at": data.get("captured_at"),
			"properties": data.get("properties"),
			"site": data.get("site"),
			"user": data.get("user"),
			"app": data.get("app"),
			"received_at": data.get("received_at"),
			"creation": data.get("received_at"),
			"modified": data.get("received_at"),
		}

	def db_update(self):
		raise NotImplementedError

	def delete(self):
		self.stream.delete_entry(self.name)

	@staticmethod
	def get_list(filters=None, page_length=None, **kwargs):
		stream = _get_event_stream()
		entries = stream.get_entries(page_length)
		return [PulseEvent._from_stream_entry(entry) for entry in entries]

	@staticmethod
	def get_etl_batch(checkpoint=None, batch_size=1000):
		stream = _get_event_stream()
		entries = stream.get_entries(min_id=checkpoint, count=batch_size, order="asc")
		events = [PulseEvent._from_stream_entry(entry) for entry in entries]
		return events

	@staticmethod
	def get_count(filters=None, **kwargs):
		stream = _get_event_stream()
		return stream.get_length()

	@staticmethod
	def get_stats(**kwargs):
		pass



def store_pulse_events():
	get_warehouse_sync_job().start_sync()


def get_warehouse_sync_job() -> WarehouseSyncJob:
	if not frappe.db.exists("Warehouse Sync Job", "Pulse Event"):
		doc = frappe.get_doc(
			{
				"doctype": "Warehouse Sync Job",
				"reference_doctype": "Pulse Event",
				"creation_key": "name",
				"primary_key": "name",
			}
		)
		doc.insert(ignore_permissions=True)
		return doc

	return frappe.get_doc("Warehouse Sync Job", "Pulse Event")
