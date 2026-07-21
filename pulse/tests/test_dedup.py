# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

"""What the derived key treats as the same event, and what it keeps apart."""

from frappe.tests import IntegrationTestCase

from pulse.dedup import dedup_key
from pulse.validation import serialize_properties

BASE = {
	"event_name": "pageview",
	"captured_at": "2026-07-20 10:00:00.123456",
	"site": "acme.frappe.cloud",
	"app": "website",
	"user": "anon_abc123",
	"team": None,
	"properties": '{"route": "/app"}',
}


def key(**overrides):
	return dedup_key(**{**BASE, **overrides})


class IntegrationTestDedupKey(IntegrationTestCase):
	def test_the_same_event_derives_the_same_key(self):
		self.assertEqual(key(), key())

	def test_every_field_changes_the_key(self):
		# Each of these makes it a different event, so none may be collapsed away.
		for field, value in (
			("event_name", "signup_viewed"),
			("captured_at", "2026-07-20 10:00:00.123457"),  # one microsecond later
			("site", "other.frappe.cloud"),
			("app", "frappe"),
			("user", "anon_def456"),
			("team", "acme"),
			("properties", '{"route": "/app/todo"}'),
		):
			self.assertNotEqual(key(), key(**{field: value}), msg=field)

	def test_property_order_does_not_change_the_key(self):
		# A client that serializes its bag in a different order on retry still has to
		# land on the same key — serialize_properties sorts, so the key is stable.
		one = serialize_properties({"route": "/app", "referrer": "https://x.test"})
		two = serialize_properties({"referrer": "https://x.test", "route": "/app"})

		self.assertEqual(key(properties=one), key(properties=two))

	def test_a_missing_field_cannot_be_forged_by_a_neighbour(self):
		# Without a separator, ("ab", None) and ("a", "b") would hash alike.
		self.assertNotEqual(key(site="ab", app=None), key(site="a", app="b"))

	def test_the_key_fits_the_column(self):
		self.assertEqual(len(key()), 32)
