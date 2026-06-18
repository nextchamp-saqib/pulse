# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import frappe
from frappe.tests import IntegrationTestCase

from pulse.api import upsert_person


def _props(doc):
	return doc.properties if isinstance(doc.properties, dict) else frappe.parse_json(doc.properties)


class IntegrationTestPulsePerson(IntegrationTestCase):
	def tearDown(self):
		frappe.db.delete("Pulse Person", {"user": ("like", "fc_test%")})
		super().tearDown()

	def test_identify_creates_person(self):
		upsert_person("fc_test_a", {"persona": "founder", "signup_source": "google"})

		doc = frappe.get_doc("Pulse Person", "fc_test_a")
		self.assertEqual(_props(doc)["persona"], "founder")

	def test_identify_merges_on_repeat(self):
		upsert_person("fc_test_b", {"persona": "founder", "signup_source": "google"})
		upsert_person("fc_test_b", {"persona": "admin", "country": "IN"})

		props = _props(frappe.get_doc("Pulse Person", "fc_test_b"))
		self.assertEqual(props["persona"], "admin")  # updated
		self.assertEqual(props["signup_source"], "google")  # kept
		self.assertEqual(props["country"], "IN")  # added

	def test_identify_requires_user(self):
		with self.assertRaises(frappe.ValidationError):
			upsert_person(None, {"persona": "founder"})
