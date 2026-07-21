# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

"""Desk view over the ingest counters (see `pulse.metrics`).

Virtual, like `Pulse Log` and `Redis Stream`: the counts live in Redis, and copying
them into a table would only create a second thing to keep true.
"""

import frappe
from frappe.model.document import Document

from pulse import metrics

# A quarter of daily rows is a short enough list to read at a glance and long
# enough to show a trend; the counters themselves expire around the same age.
MAX_DAYS = 90


class PulseIngestStat(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		accepted: DF.Int
		day: DF.Data | None
		dropped: DF.Int
		rejected: DF.Int
		total: DF.Int
	# end: auto-generated types

	def db_insert(self, *args, **kwargs):
		raise NotImplementedError

	def db_update(self, *args, **kwargs):
		raise NotImplementedError

	def delete(self):
		raise NotImplementedError

	def load_from_db(self):
		row = next((r for r in metrics.read(MAX_DAYS) if r["day"] == self.name), None)
		super(Document, self).__init__({"name": self.name, **(row or {})})

	@staticmethod
	def get_list(filters=None, page_length=20, **kwargs):
		start = int(kwargs.get("start") or 0)
		page_length = int(page_length or 20)
		rows = [{"name": row["day"], **row} for row in metrics.read(MAX_DAYS)]
		return rows[start : start + page_length]

	@staticmethod
	def get_count(filters=None, **kwargs):
		return len(metrics.read(MAX_DAYS))

	@staticmethod
	def get_stats(**kwargs):
		pass
