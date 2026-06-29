# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

"""Cookieless anonymous identity: salt rotation, id derivation, and the
server-side fill of empty-user events on browser-direct requests."""

from contextlib import contextmanager
from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase

from pulse import anon
from pulse.api import _resolve_anonymous_users


@contextmanager
def _request(ip="203.0.113.7", ua="Mozilla/5.0 (TestBrowser)"):
	"""Pin the IP + User-Agent that derive_anon_user() reads off the request."""
	saved_ip = getattr(frappe.local, "request_ip", None)
	frappe.local.request_ip = ip
	with patch("frappe.get_request_header", return_value=ua):
		try:
			yield
		finally:
			frappe.local.request_ip = saved_ip


class IntegrationTestAnon(IntegrationTestCase):
	def setUp(self):
		super().setUp()
		anon._salt_cache.clear()
		# Snapshot the site's salt so the test can mint/rotate freely and restore.
		self._saved = {
			"anon_salt": frappe.db.get_single_value("Pulse Settings", "anon_salt"),
			"anon_salt_day": frappe.db.get_single_value("Pulse Settings", "anon_salt_day"),
		}

	def tearDown(self):
		anon._salt_cache.clear()
		settings = frappe.get_doc("Pulse Settings")
		settings.anon_salt = self._saved["anon_salt"]
		settings.anon_salt_day = self._saved["anon_salt_day"]
		settings.save(ignore_permissions=True)
		frappe.db.commit()
		super().tearDown()

	# --- derive_anon_user -------------------------------------------------

	def test_id_is_anon_namespaced(self):
		with _request():
			self.assertTrue(anon.derive_anon_user("frappe.io").startswith("anon_"))

	def test_stable_within_a_day(self):
		with patch("pulse.anon._utc_day", return_value="2026-06-29"), _request():
			first = anon.derive_anon_user("frappe.io")
			anon._salt_cache.clear()  # force a fresh salt load; the value must match
			second = anon.derive_anon_user("frappe.io")
		self.assertEqual(first, second)

	def test_differs_by_site(self):
		with patch("pulse.anon._utc_day", return_value="2026-06-29"), _request():
			self.assertNotEqual(anon.derive_anon_user("frappe.io"), anon.derive_anon_user("erpnext.com"))

	def test_differs_by_ip_and_user_agent(self):
		with patch("pulse.anon._utc_day", return_value="2026-06-29"):
			with _request(ip="198.51.100.1"):
				base = anon.derive_anon_user("frappe.io")
			with _request(ip="198.51.100.2"):
				other_ip = anon.derive_anon_user("frappe.io")
			with _request(ip="198.51.100.1", ua="Mozilla/5.0 (Different)"):
				other_ua = anon.derive_anon_user("frappe.io")
		self.assertNotEqual(base, other_ip)
		self.assertNotEqual(base, other_ua)

	def test_rotates_across_days_and_discards_old_salt(self):
		with _request():
			with patch("pulse.anon._utc_day", return_value="2026-06-29"):
				day1_id = anon.derive_anon_user("frappe.io")
				salt1 = frappe.db.get_single_value("Pulse Settings", "anon_salt")

			anon._salt_cache.clear()
			with patch("pulse.anon._utc_day", return_value="2026-06-30"):
				day2_id = anon.derive_anon_user("frappe.io")
				salt2 = frappe.db.get_single_value("Pulse Settings", "anon_salt")

		# A new UTC day mints a fresh salt that overwrites the old one (discarded,
		# not recoverable), so the same visitor is a different id the next day.
		self.assertEqual(frappe.db.get_single_value("Pulse Settings", "anon_salt_day"), "2026-06-30")
		self.assertNotEqual(salt1, salt2)
		self.assertNotEqual(day1_id, day2_id)

	# --- _resolve_anonymous_users ----------------------------------------

	def test_browser_direct_fills_only_empty_user(self):
		events = [
			{"event_name": "pageview", "site": "frappe.io"},  # anon -> derived
			{"event_name": "login", "site": "frappe.io", "user": "user_7c1d"},  # identified, kept
			{"event_name": "scroll", "site": "frappe.io", "user": "anon_minted"},  # `client` mint, kept
		]
		with patch("pulse.anon._utc_day", return_value="2026-06-29"), _request():
			_resolve_anonymous_users(events, browser_direct=True)
			expected = anon.derive_anon_user("frappe.io")

		self.assertEqual(events[0]["user"], expected)  # empty -> derived
		self.assertEqual(events[1]["user"], "user_7c1d")  # stages 2-4 untouched
		self.assertEqual(events[2]["user"], "anon_minted")  # `client` opt-out untouched

	def test_derive_requires_site(self):
		# A missing site would yield a degenerate id no real event matches.
		with _request():
			for missing in (None, ""):
				with self.assertRaises(frappe.ValidationError):
					anon.derive_anon_user(missing)

	def test_siteless_event_is_left_alone(self):
		# Browser-direct but no site: skipped, not given a degenerate id (or a failure).
		events = [{"event_name": "pageview"}]
		with _request():
			_resolve_anonymous_users(events, browser_direct=True)
		self.assertNotIn("user", events[0])

	def test_server_to_server_never_derives(self):
		# s2s sends the X-Pulse-API-Key header (browser_direct=False) and always
		# supplies a user, so an empty-user event is left as-is rather than derived.
		events = [{"event_name": "pageview", "site": "frappe.io"}]
		_resolve_anonymous_users(events, browser_direct=False)
		self.assertNotIn("user", events[0])
