# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

from datetime import datetime

import frappe
from frappe.model.document import Document
from frappe.utils.logger import get_logger

from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream

logger = get_logger()


_EVENT_STREAM = None


def _get_event_stream():
	"""Return a cached Redis stream for pulse events (initialized once per process)."""
	global _EVENT_STREAM
	if _EVENT_STREAM is None:
		_EVENT_STREAM = RedisStream.init()
	return _EVENT_STREAM


class PulseEvent(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		app: DF.Data | None
		app_version: DF.Data | None
		data: DF.JSON | None
		event_name: DF.Data | None
		frappe_version: DF.Data | None
		site: DF.Data | None
		timestamp: DF.Datetime | None
	# end: auto-generated types

	@property
	def stream(self):
		return _get_event_stream()

	def validate(self):
		reqd = ["event_name", "site", "timestamp"]
		missing = [field for field in reqd if not getattr(self, field)]

		if missing:
			frappe.throw(f"Missing required fields: {', '.join(missing)}")

	def db_insert(self, *args, **kwargs):
		self.validate()
		self.stream.add(
			{
				"event_name": self.get("event_name"),
				"site": self.get("site"),
				"timestamp": self.get("timestamp"),
				"app": self.get("app"),
				"app_version": self.get("app_version"),
				"frappe_version": self.get("frappe_version"),
				"data": self.get("data"),
			}
		)

	def load_from_db(self):
		entry = self.stream.get_entry(self.name)
		doc = PulseEvent._from_stream_entry(entry)
		super(Document, self).__init__(doc)

	@staticmethod
	def _from_stream_entry(entry):
		data = entry.get("data", {})
		timestamp_ms = int(entry.get("id").split("-")[0])
		timestamp_s = timestamp_ms / 1000
		creation = datetime.fromtimestamp(timestamp_s)

		return {
			"name": entry.get("id"),
			"event_name": data.get("name") or data.get("event_name"),
			"site": data.get("site"),
			"timestamp": data.get("timestamp"),
			"app": data.get("app"),
			"app_version": data.get("app_version"),
			"frappe_version": data.get("frappe_version"),
			"data": data.get("data"),
			"creation": creation,
			"modified": creation,
		}

	def db_update(self):
		raise NotImplementedError

	def delete(self):
		self.stream.delete(self.name)

	@staticmethod
	def get_list(filters=None, page_length=None, **kwargs):
		stream = _get_event_stream()
		entries = stream.get_entries(page_length)
		return [PulseEvent._from_stream_entry(entry) for entry in entries]

	@staticmethod
	def get_etl_batch(checkpoint=None, batch_size=1000):
		stream = _get_event_stream()
		entries = stream.get_entries(min_id=checkpoint, count=batch_size, order="asc")
		events = (PulseEvent._from_stream_entry(entry) for entry in entries)
		return events

	@staticmethod
	def get_count(filters=None, **kwargs):
		stream = _get_event_stream()
		return stream.get_length()

	@staticmethod
	def get_stats(**kwargs):
		pass
