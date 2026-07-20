# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

from pulse.capture import clear_rule_cache


class PulseCaptureRule(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		action: DF.Literal["Drop", "Mark Internal"]
		disabled: DF.Check
		match_field: DF.Literal["Site", "App", "Event Name", "User"]
		match_operator: DF.Literal["Equals", "Matches"]
		match_value: DF.Data
		reason: DF.SmallText | None
	# end: auto-generated types

	def validate(self):
		if self.match_operator == "Matches" and self.match_value.strip() == "*":
			frappe.throw(
				"A rule matching '*' would apply to every event. Narrow the pattern.",
				frappe.ValidationError,
			)

	def on_update(self):
		clear_rule_cache()

	def on_trash(self):
		clear_rule_cache()
