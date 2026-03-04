# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import convert_utc_to_system_timezone, get_datetime, now_datetime


class PulseEvent(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		app: DF.Data | None
		captured_at: DF.Datetime
		event_name: DF.Data
		properties: DF.JSON | None
		received_at: DF.Datetime | None
		site: DF.Data | None
		user: DF.Data | None
	# end: auto-generated types

	def validate(self):
		captured_at = get_datetime(self.get("captured_at"))
		if captured_at.tzinfo and captured_at.tzinfo.utcoffset(None) is not None:
			captured_at = convert_utc_to_system_timezone(captured_at)
		self.captured_at = captured_at
		self.received_at = self.received_at or now_datetime()
