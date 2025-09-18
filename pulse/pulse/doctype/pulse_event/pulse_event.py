# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import convert_utc_to_system_timezone, get_datetime, now_datetime
from frappe.utils.logger import get_logger

from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream
from pulse.pulse.doctype.warehouse_sync.warehouse_sync import WarehouseSync
from pulse.utils import log_error

logger = get_logger()


_EVENT_STREAMS = {}


def _get_event_stream() -> RedisStream:
	site = getattr(frappe.local, "site", None) or "default"
	# Cache per-site to avoid returning the stream for another site
	if site not in _EVENT_STREAMS:
		_EVENT_STREAMS[site] = RedisStream.init()
	return _EVENT_STREAMS[site]


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
		captured_at = get_datetime(self.get("captured_at"))
		if captured_at.tzinfo and captured_at.tzinfo.utc:
			captured_at = convert_utc_to_system_timezone(captured_at)

		self.stream.add(
			{
				"event_name": self.get("event_name"),
				"captured_at": captured_at,
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
		entries = stream.get_entries(count=page_length)
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


@log_error()
def store_pulse_events():
	get_warehouse_sync().start_sync()


def get_warehouse_sync() -> WarehouseSync:
	if not frappe.db.exists("Warehouse Sync", "Pulse Event"):
		doc = frappe.get_doc(
			{
				"doctype": "Warehouse Sync",
				"reference_doctype": "Pulse Event",
				"creation_key": "name",
				"primary_key": "name",
			}
		)
		doc.insert(ignore_permissions=True)
		logger.info("Created Warehouse Sync for Pulse Event")
		return doc

	return frappe.get_doc("Warehouse Sync", "Pulse Event")
