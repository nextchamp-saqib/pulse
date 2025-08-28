# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import time
import uuid

import frappe
from frappe.tests import IntegrationTestCase  # type: ignore
from frappe.utils.background_jobs import get_redis_conn

import pulse.pulse.doctype.pulse_event.pulse_event as pulse_event_mod
from pulse.pulse.doctype.pulse_event.pulse_event import PulseEvent
from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream


class IntegrationTestPulseEvent(IntegrationTestCase):
	"""
	Integration tests for PulseEvent virtual doctype.
	These tests use a real Redis instance (do not mock Redis). They create a
	unique stream per test via ``frappe.flags.test_stream_name`` and clean up
	created keys after the test.
	"""

	def setUp(self):
		super().setUp()
		self.test_stream_name = f"test_pulse_event_{uuid.uuid4().hex}"
		frappe.flags.test_stream_name = self.test_stream_name
		self.stream = RedisStream.init(name=self.test_stream_name)
		self.conn = get_redis_conn()
		# ensure PulseEvent uses our test stream instance
		pulse_event_mod._EVENT_STREAM = self.stream

	def tearDown(self):
		self.stream.delete()
		pulse_event_mod._EVENT_STREAM = None
		frappe.flags.test_stream_name = None
		super().tearDown()

	def test_validate_throws_when_missing_required_fields(self):
		doc = frappe.new_doc("Pulse Event")
		with self.assertRaises(frappe.ValidationError):
			doc.validate()

	def test_db_insert_adds_entry_to_stream(self):
		# create a PulseEvent document and call db_insert which should add
		# an entry to the underlying Redis stream
		# then ensure stream length increased and get_count via PulseEvent returns >= 1
		pe = frappe.get_doc(
			{
				"doctype": "Pulse Event",
				"event_name": "integration_test_event",
				"captured_at": frappe.utils.now_datetime(),
				"subject_id": "sub-1",
				"subject_type": "TestType",
				"props": {"key": "value"},
			}
		)
		pe.db_insert()

		length = self.stream.get_length()
		self.assertGreaterEqual(length, 1)
		self.assertGreaterEqual(PulseEvent.get_count(), 1)

	def test_load_from_db_and_get_entry(self):
		# add a raw entry to stream and then use PulseEvent.load_from_db to
		# populate a Document from that entry
		payload = {
			"event_name": "load_test",
			"subject_id": "s1",
			"subject_type": "T",
			"captured_at": frappe.utils.now_datetime(),
			"props": {},
		}
		self.stream.add(payload)

		# fetch the newest entry id
		entries = self.stream.get_entries(count=1)
		self.assertTrue(entries)
		entry = entries[0]

		# loaded document should have matching fields
		pe = frappe.get_doc("Pulse Event", entry.get("id"))
		self.assertEqual(pe.event_name, payload.get("event_name"))
		self.assertEqual(pe.subject_id, payload.get("subject_id"))

	def test_get_list_and_get_etl_batch(self):
		# add a few entries and test get_etl_batch generator
		for i in range(3):
			self.stream.add(
				{
					"event_name": f"etl_{i}",
					"subject_id": str(i),
					"subject_type": "T",
					"captured_at": frappe.utils.now_datetime(),
				}
			)

		# get an iterator of events from checkpoint None
		events = PulseEvent.get_etl_batch(checkpoint=None, batch_size=10)
		# materialize first few events
		collected = list(events)
		self.assertGreaterEqual(len(collected), 1)

		# capture the last id from the batch as a checkpoint
		last_id = collected[-1].get("name")

		# now add a few more entries after checkpoint
		for i in range(2):
			self.stream.add({
				"event_name": f"etl_after_{i}",
				"subject_id": f"after_{i}",
				"subject_type": "T",
				"captured_at": frappe.utils.now_datetime(),
			})

		new_events = list(PulseEvent.get_etl_batch(checkpoint=last_id, batch_size=10))
		# expect at least the newly added events to be present
		self.assertGreaterEqual(len(new_events), 2)

	def test_delete_removes_stream_key(self):
		# add an entry, then delete via RedisStream.delete and assert key gone
		self.stream.add(
			{
				"event_name": "del_test",
				"subject_id": "d1",
				"subject_type": "T",
				"captured_at": frappe.utils.now_datetime(),
			}
		)
		# ensure key exists
		self.assertTrue(self.conn.exists(self.stream.key))
		# delete and assert key no longer exists
		self.stream.delete()
		self.assertFalse(self.conn.exists(self.stream.key))
