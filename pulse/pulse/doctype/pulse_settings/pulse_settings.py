# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

# import frappe
from frappe.model.document import Document


class PulseSettings(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		api_key: DF.Password | None
		max_stream_length: DF.Int
		rate_limit: DF.Int
	# end: auto-generated types

	pass
