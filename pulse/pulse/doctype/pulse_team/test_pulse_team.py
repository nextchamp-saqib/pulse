# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import frappe
from frappe.tests import IntegrationTestCase

from pulse.api import upsert_team


def _props(doc):
	return doc.properties if isinstance(doc.properties, dict) else frappe.parse_json(doc.properties)


class IntegrationTestPulseTeam(IntegrationTestCase):
	def tearDown(self):
		frappe.db.delete("Pulse Team", {"team": ("like", "fc_test%")})
		super().tearDown()

	def test_identify_creates_team(self):
		upsert_team("fc_test_a", {"persona": "founder", "signup_source": "google"})

		doc = frappe.get_doc("Pulse Team", "fc_test_a")
		self.assertEqual(_props(doc)["persona"], "founder")

	def test_identify_merges_on_repeat(self):
		upsert_team("fc_test_b", {"persona": "founder", "signup_source": "google"})
		upsert_team("fc_test_b", {"persona": "admin", "country": "IN"})

		props = _props(frappe.get_doc("Pulse Team", "fc_test_b"))
		self.assertEqual(props["persona"], "admin")  # updated
		self.assertEqual(props["signup_source"], "google")  # kept
		self.assertEqual(props["country"], "IN")  # added

	def test_identify_requires_team(self):
		with self.assertRaises(frappe.ValidationError):
			upsert_team(None, {"persona": "founder"})
