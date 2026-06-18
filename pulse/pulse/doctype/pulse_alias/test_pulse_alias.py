# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import frappe
from frappe.tests import IntegrationTestCase

from pulse.api import upsert_alias


class IntegrationTestPulseAlias(IntegrationTestCase):
	def tearDown(self):
		frappe.db.delete("Pulse Alias", {"previous_id": ("like", "anon_test%")})
		super().tearDown()

	def test_alias_creates_mapping(self):
		upsert_alias("anon_test_a", "fc_priya")

		doc = frappe.get_doc("Pulse Alias", "anon_test_a")
		self.assertEqual(doc.user, "fc_priya")

	def test_alias_updates_existing(self):
		upsert_alias("anon_test_b", "fc_temp")
		upsert_alias("anon_test_b", "fc_priya")

		doc = frappe.get_doc("Pulse Alias", "anon_test_b")
		self.assertEqual(doc.user, "fc_priya")

	def test_alias_requires_both(self):
		with self.assertRaises(frappe.ValidationError):
			upsert_alias("anon_test_c", None)
