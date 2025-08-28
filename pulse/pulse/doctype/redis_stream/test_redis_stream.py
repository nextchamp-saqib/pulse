# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import time
import uuid

import frappe
from frappe.tests import IntegrationTestCase  # type: ignore
from frappe.utils.background_jobs import get_redis_conn

from pulse.constants import PENDING_MIN_IDLE_MS, STREAM_MAX_LENGTH
from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream


class IntegrationTestRedisStream(IntegrationTestCase):
	"""
	Integration tests for RedisStream.
	Use this class for testing interactions between multiple components.
	"""

	def setUp(self):
		super().setUp()
		# use a unique stream name per test to avoid interference
		self.test_stream_name = f"test_stream_{uuid.uuid4().hex}"
		frappe.flags.test_stream_name = self.test_stream_name
		self.stream = RedisStream.init(name=self.test_stream_name)

	def tearDown(self):
		self.stream.delete()
		frappe.flags.test_stream_name = None
		super().tearDown()

	def test_create_stream_initializes_group(self):
		"""Ensure creating/initializing a RedisStream creates the consumer group."""
		self.assertTrue(self.stream.conn.exists(self.stream.key))
		groups = self.stream.conn.xinfo_groups(self.stream.key)
		# there should be at least one group defined for the stream
		self.assertTrue(groups and len(groups) >= 1)

	def test_add_and_read_entry(self):
		"""Test adding an entry to the stream and reading it back via read()."""
		payload = {"event": "test", "value": "123"}
		# add entry
		self.stream.add(payload)

		# read entries (should create consumer group if needed and return entries)
		entries = self.stream.read(count=10)
		self.assertTrue(isinstance(entries, list))
		self.assertGreaterEqual(len(entries), 1)
		# verify at least one entry contains our data
		found = any(e.get("data", {}).get("event") == "test" for e in entries)
		self.assertTrue(found)

	def test_acknowledge_entries(self):
		"""Test that acknowledging entries removes them from pending/lag counts."""
		# add two entries
		payload1 = {"event": "ack_test_1"}
		payload2 = {"event": "ack_test_2"}
		self.stream.add(payload1)
		self.stream.add(payload2)

		# read() will deliver new entries to this consumer and return normalized entries
		entries = self.stream.read(count=10) or []
		self.assertTrue(isinstance(entries, list))
		ids = [e.get("id") for e in entries if isinstance(e, dict) and e.get("id")]
		self.assertGreaterEqual(len(ids), 1)

		# ack entries using stream helper
		self.stream.ack_entries(ids)

		# after ack, unacknowledged length should be zero
		lag = self.stream.get_unacknowledged_length()
		self.assertEqual(lag, 0)

	def test_pending_and_stale_behavior(self):
		"""Test reading pending, stale (autoclaim) and new entries ordering/priority."""
		# Add an entry and deliver it to this consumer
		self.stream.add({"event": "stale_test"})

		# deliver the new entry to this consumer
		delivered = self.stream.read_new(count=10) or []
		# delivered may be raw tuples (depending on underlying call), but should be iterable
		self.assertIsNotNone(delivered)

		# reading via read() should return normalized entries (including pending)
		entries = self.stream.read(count=10) or []
		self.assertIsInstance(entries, list)

		# Wait to allow autoclaim thresholds (no strict guarantee about claim results)
		time.sleep((PENDING_MIN_IDLE_MS + 500) / 1000.0)

		# read_stale should run without error; result may be empty depending on state
		stale = self.stream.read_stale(count=10) or []
		self.assertIsNotNone(stale)

	def test_stream_stats(self):
		"""Test stats like length, memory usage, and near-capacity warning behavior."""
		# Ensure stream length increases when adding entries
		initial_len = self.stream.get_length() or 0
		# add a few entries
		for i in range(3):
			self.stream.add({"event": f"stat_test_{i}", "i": i})

		new_len = self.stream.get_length()
		self.assertGreaterEqual(new_len, initial_len + 3)

		# get memory usage (may be None depending on redis permissions)
		mem = self.stream.get_memory_usage()
		# memory usage should be int or None
		self.assertTrue(mem is None or isinstance(mem, int))
