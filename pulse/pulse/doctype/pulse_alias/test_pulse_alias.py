# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import frappe
from frappe.tests import IntegrationTestCase

from pulse.api import upsert_alias, upsert_team


class IntegrationTestPulseAlias(IntegrationTestCase):
	"""The alias endpoint accepts the public ingest key, so these guards — not the
	key — are what stop a caller from re-pointing an established id or collapsing
	two identities (PostHog's "only anon -> identified" merge rule)."""

	def tearDown(self):
		frappe.db.delete("Pulse Alias", {"previous_id": ("like", "anon_test%")})
		frappe.db.delete("Pulse Alias", {"previous_id": ("like", "fc_test%")})
		frappe.db.delete("Pulse Team", {"team": ("like", "fc_test%")})
		super().tearDown()

	def test_alias_creates_mapping(self):
		upsert_alias("anon_test_a", "fc_priya")

		doc = frappe.get_doc("Pulse Alias", "anon_test_a")
		self.assertEqual(doc.team, "fc_priya")

	def test_alias_refuses_repoint_to_different_identity(self):
		# Once an id is linked, it can't be re-pointed to another identity.
		upsert_alias("anon_test_b", "fc_temp")
		upsert_alias("anon_test_b", "fc_priya")  # refused

		doc = frappe.get_doc("Pulse Alias", "anon_test_b")
		self.assertEqual(doc.team, "fc_temp")

	def test_alias_refuses_when_previous_id_is_already_an_identity(self):
		# An identified person (has a profile) can't be merged under another id.
		upsert_team("fc_test_known", {"plan": "pro"})
		upsert_alias("fc_test_known", "fc_other")  # refused

		self.assertIsNone(frappe.db.get_value("Pulse Alias", "fc_test_known", "team"))

	def test_alias_requires_both(self):
		with self.assertRaises(frappe.ValidationError):
			upsert_alias("anon_test_c", None)
