# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import frappe
from frappe.tests import IntegrationTestCase  # type: ignore
from frappe.utils import now_datetime


class IntegrationTestPulseEvent(IntegrationTestCase):
	def test_validate_throws_when_missing_required_fields(self):
		doc = frappe.new_doc("Pulse Event")
		with self.assertRaises(frappe.ValidationError):
			doc.validate()

	def test_db_insert_saves_to_mariadb(self):
		doc = frappe.get_doc(
			{
				"doctype": "Pulse Event",
				"event_name": "test_event",
				"captured_at": now_datetime(),
				"site": "test-site",
				"app": "test-app",
				"user": "test-user",
				"properties": {"key": "value"},
			}
		)
		doc.insert(ignore_permissions=True)
		self.assertTrue(frappe.db.exists("Pulse Event", doc.name))

		# cleanup
		frappe.delete_doc("Pulse Event", doc.name, ignore_permissions=True)

	def test_received_at_set_automatically(self):
		doc = frappe.get_doc(
			{
				"doctype": "Pulse Event",
				"event_name": "auto_ts_event",
				"captured_at": now_datetime(),
			}
		)
		doc.insert(ignore_permissions=True)
		self.assertIsNotNone(doc.received_at)

		# cleanup
		frappe.delete_doc("Pulse Event", doc.name, ignore_permissions=True)
