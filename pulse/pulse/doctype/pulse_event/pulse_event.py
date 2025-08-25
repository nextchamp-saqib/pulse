# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import now
from frappe.utils.data import make_filter_dict
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
				"creation": now()
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
			"event_name": data.get("name") or data.get("event_name"),
			"site": data.get("site"),
			"timestamp": data.get("timestamp"),
			"app": data.get("app"),
			"app_version": data.get("app_version"),
			"frappe_version": data.get("frappe_version"),
			"data": data.get("data"),
			"creation": data.get("creation")
		}

	def db_update(self):
		raise NotImplementedError

	def delete(self):
		self.stream.delete(self.name)

	@staticmethod
	def get_list(filters=None, order_by=None, page_length=None, limit_page_length=None, **kwargs):
		filters = filters or {}
		if isinstance(filters, list):
			filters = make_filter_dict(filters)

		min_id = None
		max_id = None
		name_filter = filters.get("name")
		name_filter_op = None
		name_filter_val = None
		if isinstance(name_filter, list | tuple) and len(name_filter) == 2:
			name_filter_op = name_filter[0]
			name_filter_val = name_filter[1]
		elif isinstance(name_filter, str | int):
			name_filter_op = "="
			name_filter_val = name_filter

		if name_filter_op and name_filter_val:
			if name_filter_op == "=":
				min_id = name_filter_val
				max_id = name_filter_val
			elif name_filter_op in (">", ">="):
				min_id = name_filter_val
				max_id = None
			elif name_filter_op in ("<", "<="):
				min_id = None
				max_id = name_filter_val
			else:
				min_id = None
				max_id = None

		order_by = order_by.lower()
		order = "asc" if "asc" in order_by else "desc"

		stream = _get_event_stream()
		count = page_length or limit_page_length
		entries = stream.get_entries(min_id=min_id, max_id=max_id, count=count, order=order)
		return [PulseEvent._from_stream_entry(entry) for entry in entries]

	@staticmethod
	def get_count(filters=None, **kwargs):
		stream = _get_event_stream()
		return stream.get_length()

	@staticmethod
	def get_stats(**kwargs):
		pass
