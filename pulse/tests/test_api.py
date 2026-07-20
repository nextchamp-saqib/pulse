# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

"""Batch ingest: how a bad event in a batch is reported without losing the good ones."""

import uuid
from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase

from pulse.api import _bulk_ingest
from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream


class IntegrationTestBulkIngest(IntegrationTestCase):
	def setUp(self):
		super().setUp()
		token = uuid.uuid4().hex
		self.prefix = f"test_{token}_"
		frappe.flags.test_stream_name = f"test_bulk_ingest_{token}"
		self.stream = RedisStream.init()
		# The endpoint's auth is exercised in its own right; these tests are about
		# what happens to a batch once it is past the door.
		self.auth = patch("pulse.api.check_auth", lambda: None)
		self.auth.start()

	def tearDown(self):
		self.auth.stop()
		self.stream.delete()
		frappe.flags.test_stream_name = None
		super().tearDown()

	def _event(self, name, **kwargs):
		kwargs.setdefault("captured_at", str(frappe.utils.now_datetime()))
		return {"event_name": f"{self.prefix}{name}", "site": "test-site", **kwargs}

	def test_rejects_bad_event_and_keeps_the_rest(self):
		batch = [
			self._event("good_one"),
			{"event_name": None, "captured_at": str(frappe.utils.now_datetime())},
			self._event("good_two"),
		]

		result = _bulk_ingest(batch, browser_direct=False)

		self.assertEqual(result["accepted"], 2)
		self.assertEqual(len(result["rejected"]), 1)
		# The index locates the offender in the batch the client sent.
		self.assertEqual(result["rejected"][0]["index"], 1)
		self.assertEqual(self.stream.get_length(), 2)

	def test_infrastructure_failure_fails_the_whole_request(self):
		# Unlike a rejected event, this one the client *should* retry — so it must
		# surface as a failed request rather than a per-event rejection.
		with patch("pulse.api.enqueue_event", side_effect=RuntimeError("redis is down")):
			with self.assertRaises(RuntimeError):
				_bulk_ingest([self._event("boom")], browser_direct=False)
