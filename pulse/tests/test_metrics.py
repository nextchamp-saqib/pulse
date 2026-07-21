# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

"""Ingest counters: what they record, and that they never cost an event."""

import uuid
from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase
from frappe.utils import getdate, now_datetime

from pulse import metrics
from pulse.api import _bulk_ingest
from pulse.capture import DROP, clear_rule_cache
from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream


class IntegrationTestMetrics(IntegrationTestCase):
	def setUp(self):
		super().setUp()
		token = uuid.uuid4().hex
		self.prefix = f"test_{token}_"
		frappe.flags.test_stream_name = f"test_metrics_{token}"
		self.stream = RedisStream.init()
		self.auth = patch("pulse.api.check_auth", lambda: None)
		self.auth.start()
		self.today = str(getdate(now_datetime()))
		self._clear()
		self.rules = []

	def tearDown(self):
		self._clear()
		for rule in self.rules:
			frappe.delete_doc("Pulse Capture Rule", rule, force=True, ignore_permissions=True)
		clear_rule_cache()
		self.auth.stop()
		self.stream.delete()
		frappe.flags.test_stream_name = None
		frappe.db.commit()
		super().tearDown()

	def _clear(self):
		for outcome in metrics.OUTCOMES:
			frappe.cache.delete(metrics._key(self.today, outcome))

	def _today(self):
		return next((row for row in metrics.read(7) if row["day"] == self.today), None)

	def _event(self, name, **kwargs):
		kwargs.setdefault("captured_at", str(now_datetime()))
		return {"event_name": f"{self.prefix}{name}", "site": "real.test", **kwargs}

	def test_counts_each_outcome_of_a_batch(self):
		rule = frappe.get_doc(
			{
				"doctype": "Pulse Capture Rule",
				"match_field": "Site",
				"match_operator": "Equals",
				"match_value": "noisy.test",
				"action": DROP,
			}
		).insert(ignore_permissions=True)
		self.rules.append(rule.name)
		clear_rule_cache()

		_bulk_ingest(
			[
				self._event("kept"),
				self._event("kept_too"),
				self._event("noise", site="noisy.test"),
				{"event_name": "NOT A VALID NAME", "captured_at": str(now_datetime())},
			],
			browser_direct=False,
		)

		row = self._today()
		self.assertEqual(row["accepted"], 2)
		self.assertEqual(row["dropped"], 1)
		self.assertEqual(row["rejected"], 1)
		self.assertEqual(row["total"], 4)

	def test_counts_accumulate_across_batches(self):
		_bulk_ingest([self._event("one")], browser_direct=False)
		_bulk_ingest([self._event("two")], browser_direct=False)

		self.assertEqual(self._today()["accepted"], 2)

	def test_a_day_with_no_traffic_is_not_listed(self):
		self.assertIsNone(self._today())

	def test_a_broken_counter_does_not_break_ingest(self):
		# The counters are bookkeeping; Redis failing to count must not cost an event.
		with patch.object(frappe.cache, "incrby", side_effect=RuntimeError("redis is down")):
			result = _bulk_ingest([self._event("survives")], browser_direct=False)

		self.assertEqual(result["accepted"], 1)
		self.assertEqual(self.stream.get_length(), 1)
