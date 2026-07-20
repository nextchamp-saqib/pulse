# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import uuid
from unittest.mock import patch

import frappe
from frappe.tests import IntegrationTestCase  # type: ignore
from frappe.utils.background_jobs import get_redis_conn

from pulse.pulse.doctype.pulse_event.pulse_event import (
	consume_pulse_events,
	enqueue_event,
)
from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream


class IntegrationTestPulseEvent(IntegrationTestCase):
	"""
	Integration tests for the Pulse Event ingest -> stream -> database pipeline.
	These tests use a real Redis instance (do not mock Redis). Each test gets a
	unique stream (via ``frappe.flags.test_stream_name``) and tags its events with
	a unique ``event_name`` prefix, so assertions and cleanup are scoped to the
	test's own rows and never touch existing site data.
	"""

	def setUp(self):
		super().setUp()
		token = uuid.uuid4().hex
		self.test_stream_name = f"test_pulse_event_{token}"
		self.prefix = f"test_{token}_"
		frappe.flags.test_stream_name = self.test_stream_name
		self.stream = RedisStream.init(name=self.test_stream_name)
		self.conn = get_redis_conn()

	def tearDown(self):
		self.stream.delete()
		frappe.flags.test_stream_name = None
		# Only remove rows this test created; leave existing site data untouched.
		frappe.db.delete("Pulse Event", {"event_name": ("like", f"{self.prefix}%")})
		frappe.db.commit()
		super().tearDown()

	def _enqueue(self, name, **kwargs):
		kwargs.setdefault("captured_at", frappe.utils.now_datetime())
		enqueue_event(event_name=f"{self.prefix}{name}", **kwargs)

	def _count(self):
		return frappe.db.count("Pulse Event", {"event_name": ("like", f"{self.prefix}%")})

	def test_enqueue_validates_required_fields(self):
		with self.assertRaises(frappe.ValidationError):
			enqueue_event(event_name=None, captured_at=frappe.utils.now_datetime())
		with self.assertRaises(frappe.ValidationError):
			enqueue_event(event_name="x", captured_at=None)

	def test_enqueue_adds_entry_to_stream(self):
		self._enqueue(
			"signup",
			site="test-site",
			app="test-app",
			user="anon_testuser",
			team="team_test",
			properties={"key": "value"},
		)
		self.assertGreaterEqual(self.stream.get_length(), 1)

	def test_consume_writes_events_to_database(self):
		for i in range(3):
			self._enqueue(f"evt_{i}", site=f"site_{i}", properties={"i": i})

		consume_pulse_events()

		rows = frappe.get_all(
			"Pulse Event",
			filters={"event_name": ("like", f"{self.prefix}%")},
			fields=["name", "event_name", "site", "properties"],
		)
		self.assertEqual(len(rows), 3)
		names = {r.event_name for r in rows}
		self.assertEqual(names, {f"{self.prefix}evt_{i}" for i in range(3)})
		# name is the stream entry id; properties round-trips as valid JSON
		row = next(r for r in rows if r.event_name == f"{self.prefix}evt_1")
		self.assertEqual(frappe.parse_json(row.properties), {"i": 1})

	def test_consume_is_idempotent_on_redelivery(self):
		self._enqueue("once", site="s")

		# Simulate a crash after the insert commits but before XACK: the row is
		# written, but the entry stays pending and is redelivered on the next drain.
		with patch.object(RedisStream, "ack_entries", lambda self, ids: None):
			consume_pulse_events()
		self.assertEqual(self._count(), 1)

		# Second drain re-reads the pending entry and re-inserts it. ignore_duplicates
		# on the entry-id primary key keeps the table at exactly one row.
		consume_pulse_events()
		self.assertEqual(self._count(), 1)

	def test_consume_no_events_is_noop(self):
		consume_pulse_events()
		self.assertEqual(self._count(), 0)

	def test_consume_keeps_one_row_per_client_event_id(self):
		# A client that resends a batch — after a timeout, or because it double-fired —
		# sends the same event_id again. Each copy is a separate stream entry, so the
		# entry-id primary key can't catch it; the unique event_id is what does.
		event_id = uuid.uuid4().hex
		self._enqueue("resent", site="s", event_id=event_id)
		self._enqueue("resent", site="s", event_id=event_id)

		consume_pulse_events()

		self.assertEqual(self._count(), 1)

	def test_consume_keeps_every_event_without_an_id(self):
		# Callers that send no event_id opt out of deduplication rather than
		# collapsing onto a shared empty key.
		self._enqueue("unkeyed", site="s")
		self._enqueue("unkeyed", site="s")

		consume_pulse_events()

		self.assertEqual(self._count(), 2)
