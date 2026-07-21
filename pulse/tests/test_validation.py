# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

"""What the ingest door lets through, what it repairs, and what it turns away."""

import datetime
from zoneinfo import ZoneInfo

import frappe
from frappe.tests import IntegrationTestCase
from frappe.utils import get_system_timezone, now_datetime

from pulse.constants import MAX_PROPERTIES_LENGTH, MAX_PROPERTY_VALUE_LENGTH
from pulse.validation import resolve_captured_at, serialize_properties, validate_event_name


class IntegrationTestEventName(IntegrationTestCase):
	def test_accepts_deliberate_names(self):
		for name in ("pageview", "ticket_created", "helpdesk:ticket_created", "a1_b2"):
			validate_event_name(name)

	def test_rejects_names_built_at_runtime(self):
		names = [
			# Translated UI copy used as the event key — one key per language.
			"dismissed_let's_set_up_the_stock_module.",
			"dismissed_haydi_erpnext_ile_yolculuğa_başlayalım!",  # noqa: RUF001
			# A doctype concatenated onto a literal.
			"bulk_deleteHD Ticket",
			# An injection probe fired at the public endpoint.
			"email_sent' AND pg_sleep(20)--",
			"Signup_Viewed",
			"event name with spaces",
			"",
			"9lives",
			"x",
			"a" * 65,
		]
		for name in names:
			with self.assertRaises(frappe.ValidationError, msg=name):
				validate_event_name(name)


class IntegrationTestCapturedAt(IntegrationTestCase):
	def setUp(self):
		super().setUp()
		self.received = now_datetime()

	def test_keeps_a_plausible_timestamp(self):
		captured = self.received - datetime.timedelta(minutes=30)
		self.assertEqual(resolve_captured_at(captured, self.received), captured)

	def test_tolerates_small_clock_skew(self):
		captured = self.received + datetime.timedelta(minutes=2)
		self.assertEqual(resolve_captured_at(captured, self.received), captured)

	def test_clamps_a_clock_running_ahead(self):
		captured = self.received + datetime.timedelta(days=21)
		self.assertEqual(resolve_captured_at(captured, self.received), self.received)

	def test_rejects_an_ancient_timestamp(self):
		with self.assertRaises(frappe.ValidationError):
			resolve_captured_at(datetime.datetime(1970, 1, 1), self.received)

	def test_rejects_an_unparseable_timestamp(self):
		with self.assertRaises(frappe.ValidationError):
			resolve_captured_at("not a datetime", self.received)

	def test_normalizes_an_aware_timestamp_to_naive_system_time(self):
		# The browser sends `new Date().toISOString()` — UTC with an offset. It has to
		# land as naive system time, which is what the column and `received_at` are.
		system = self.received.replace(tzinfo=ZoneInfo(get_system_timezone()))
		resolved = resolve_captured_at(system.astimezone(ZoneInfo("UTC")), self.received)

		self.assertIsNone(resolved.tzinfo)
		self.assertAlmostEqual(resolved, self.received, delta=datetime.timedelta(seconds=1))


class IntegrationTestProperties(IntegrationTestCase):
	def test_empty_and_non_dict_become_an_empty_bag(self):
		for value in (None, "", [1, 2], "not json"):
			self.assertEqual(frappe.parse_json(serialize_properties(value)), {})

	def test_accepts_a_json_string(self):
		self.assertEqual(frappe.parse_json(serialize_properties('{"route": "/app"}')), {"route": "/app"})

	def test_truncates_a_long_value_but_keeps_the_key(self):
		parsed = frappe.parse_json(serialize_properties({"route": "/app", "html": "x" * 5000}))

		self.assertEqual(parsed["route"], "/app")
		self.assertEqual(len(parsed["html"]), MAX_PROPERTY_VALUE_LENGTH)

	def test_truncates_long_values_nested_in_a_document(self):
		# The shape that produced 41KB rows: a whole form doc under one key.
		doc = {"data": {"description": "<p>" + "x" * 40000, "title": "AAC 14"}}
		parsed = frappe.parse_json(serialize_properties(doc))

		self.assertEqual(parsed["data"]["title"], "AAC 14")
		self.assertEqual(len(parsed["data"]["description"]), MAX_PROPERTY_VALUE_LENGTH)

	def test_replaces_a_bag_that_is_still_too_large(self):
		serialized = serialize_properties({f"key_{i}": "x" * 400 for i in range(50)})
		parsed = frappe.parse_json(serialized)

		self.assertTrue(parsed["_truncated"])
		self.assertLess(len(serialized), MAX_PROPERTIES_LENGTH)
