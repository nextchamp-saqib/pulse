# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

from frappe.model.document import Document


class PulsePerson(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		properties: DF.JSON | None
		user: DF.Data
	# end: auto-generated types

	pass
