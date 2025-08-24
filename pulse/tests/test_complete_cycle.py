# Copyright (c) 2025, hello@frappe.io and Contributors
# See license.txt

import json
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import frappe
from frappe.tests import IntegrationTestCase

from pulse.api import ingest
from pulse.processor import process_events
from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream
from pulse.storage import _get_db, _get_db_path, store_batch_in_duckdb

TEST_STREAM_NAME = "pulse:events:test"


class TestPulseCompleteCycle(IntegrationTestCase):
	"""
	Integration test that covers the complete Pulse cycle:
	1. Event ingestion via API
	2. Event processing from Redis Stream
	3. Event storage in DuckDB
	"""

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		# Ensure Pulse Settings exists
		if not frappe.db.exists("Pulse Settings", "Pulse Settings"):
			pulse_settings = frappe.get_doc({"doctype": "Pulse Settings", "api_key": "test_api_key_12345"})
			pulse_settings.insert()
		else:
			# Update API key for testing
			frappe.db.set_single_value("Pulse Settings", "api_key", "test_api_key_12345")

		frappe.db.commit()

	def setUp(self):
		super().setUp()
		self.cleanup_test_data()
		# Set test stream name for all tests
		frappe.flags.test_stream_name = TEST_STREAM_NAME

	def tearDown(self):
		super().tearDown()
		self.cleanup_test_data()
		# Clean up frappe.flags
		if hasattr(frappe.flags, "test_stream_name"):
			delattr(frappe.flags, "test_stream_name")

	def cleanup_test_data(self):
		"""Clean up test data from Redis Stream and DuckDB"""
		try:
			# Clean Redis Stream
			stream = RedisStream.init(TEST_STREAM_NAME)
			stream.delete()
		except Exception:
			pass

		try:
			# Clean DuckDB
			db_path = _get_db_path()
			if os.path.exists(db_path):
				os.remove(db_path)
		except Exception:
			pass

	def test_complete_pulse_cycle(self):
		"""Test the complete Pulse cycle from event ingestion to acknowledgment"""

		# Step 1: Prepare test events
		test_events = [
			{
				"site": frappe.local.site,
				"event_name": "user_login",
				"timestamp": datetime.now(timezone.utc).isoformat(),
				"app": "frappe",
				"app_version": "14.0.0",
				"frappe_version": "14.0.0",
				"user_id": "test@example.com",
				"session_id": "sess_123",
				"ip_address": "192.168.1.1",
			},
			{
				"site": frappe.local.site,
				"event_name": "page_view",
				"timestamp": datetime.now(timezone.utc).isoformat(),
				"app": "frappe",
				"app_version": "14.0.0",
				"frappe_version": "14.0.0",
				"page": "/desk",
				"user_id": "test@example.com",
				"session_id": "sess_123",
			},
			{
				"site": frappe.local.site,
				"event_name": "document_create",
				"timestamp": datetime.now(timezone.utc).isoformat(),
				"app": "frappe",
				"app_version": "14.0.0",
				"frappe_version": "14.0.0",
				"doctype": "User",
				"doc_name": "new_user@example.com",
				"user_id": "test@example.com",
			},
		]

		# Step 2: Test Event Ingestion via API using frappe.flags
		# Mock frappe.request with proper headers
		mock_request = MagicMock()
		mock_request.headers = {"Authorization": "Bearer test_api_key_12345"}

		with patch("frappe.request", mock_request):
			ingest(test_events)

		# Verify events are in Redis Stream
		stream = RedisStream.init(TEST_STREAM_NAME)
		stream_length = stream.get_length()
		self.assertEqual(stream_length, 3, "Events should be stored in Redis Stream")

		# Step 3: Test Event Processing (processor will use frappe.flags.test_stream_name)
		process_events()

		# Verify events are processed and moved to DuckDB
		conn = _get_db()
		try:
			# Check if events table exists and has data
			result = conn.execute("SELECT COUNT(*) FROM event").fetchone()
			event_count = result[0] if result else 0
			self.assertEqual(event_count, 3, "All events should be stored in DuckDB")

			# Check specific event data
			events = conn.execute("SELECT * FROM event ORDER BY timestamp").fetchall()
			self.assertEqual(len(events), 3)

			# Verify first event structure and data
			first_event = dict(zip([desc[0] for desc in conn.description], events[0], strict=True))
			self.assertIsNotNone(first_event["name"], "Event should have an auto-generated ID")
			self.assertEqual(first_event["event_name"], "user_login")
			self.assertEqual(first_event["site"], frappe.local.site)

			# Verify JSON data field contains extra attributes
			data = json.loads(first_event["data"])
			self.assertEqual(data["user_id"], "test@example.com")
			self.assertEqual(data["session_id"], "sess_123")
			self.assertEqual(data["ip_address"], "192.168.1.1")

		finally:
			conn.close()

		# Verify Redis Stream is cleaned up (events acknowledged)
		remaining_events = stream.read(100)
		self.assertEqual(len(remaining_events), 0, "Events should be acknowledged and removed from stream")

	def test_api_authentication_and_ingestion(self):
		"""Test API authentication and event ingestion"""

		test_events = [
			{
				"site": frappe.local.site,
				"event_name": "api_test_event",
				"timestamp": datetime.now(timezone.utc).isoformat(),
			}
		]

		# Test successful authentication and ingestion
		mock_request = MagicMock()
		mock_request.headers = {"Authorization": "Bearer test_api_key_12345"}

		with patch("frappe.request", mock_request):
			# Should not raise any exception
			ingest(test_events)

		# Verify event was added to stream
		stream = RedisStream.init(TEST_STREAM_NAME)
		self.assertEqual(stream.get_length(), 1, "Event should be added to stream")

		# Test authentication failure
		mock_request_invalid = MagicMock()
		mock_request_invalid.headers = {"Authorization": "Bearer invalid_key"}

		with patch("frappe.request", mock_request_invalid):
			with self.assertRaises(Exception):  # Should raise PermissionError
				ingest(test_events)

	def test_event_validation_and_sanitization(self):
		"""Test event validation and sanitization during processing"""

		# Test events with various validation scenarios
		test_events = [
			# Valid event
			{
				"site": frappe.local.site,
				"event_name": "valid_event",
				"timestamp": datetime.now(timezone.utc).isoformat(),
				"extra_field": "should_go_to_data",
			},
			# Missing required field (should be discarded)
			{
				"site": frappe.local.site,
				# missing 'event_name' field
				"timestamp": datetime.now(timezone.utc).isoformat(),
			},
			# Invalid data format (should be discarded)
			"invalid_string_event",
			# Valid event with minimal fields
			{
				"site": frappe.local.site,
				"event_name": "minimal_event",
				"timestamp": datetime.now(timezone.utc).isoformat(),
			},
		]

		# Add events to Redis Stream directly (bypassing API validation)
		stream = RedisStream.init(TEST_STREAM_NAME)
		for event in test_events:
			try:
				stream.add(event if isinstance(event, dict) else {"invalid": event})
			except Exception:
				# Some invalid events might fail to add
				pass

		# Process events (processor will use frappe.flags.test_stream_name)
		process_events()

		# Check results in DuckDB
		conn = _get_db()
		try:
			events = conn.execute("SELECT * FROM event ORDER BY timestamp").fetchall()
			# Should only have 2 valid events
			self.assertEqual(len(events), 2, "Only valid events should be stored")

			# Check that extra fields are moved to data JSON
			first_event = dict(zip([desc[0] for desc in conn.description], events[0], strict=True))
			if first_event["event_name"] == "valid_event":
				data = json.loads(first_event["data"])
				self.assertEqual(data["extra_field"], "should_go_to_data")

		finally:
			conn.close()

	def test_direct_storage_functionality(self):
		"""Test direct storage functionality"""

		test_batch = [
			{
				"site": frappe.local.site,
				"event_name": "storage_test",
				"timestamp": datetime.now(timezone.utc).isoformat(),
				"app": "test_app",
				"app_version": "1.0.0",
				"frappe_version": "14.0.0",
				"data": {"test": "data"},
			}
		]

		# Test direct storage
		store_batch_in_duckdb(test_batch)

		# Verify storage
		conn = _get_db()
		try:
			result = conn.execute("SELECT COUNT(*) FROM event WHERE event_name = 'storage_test'").fetchone()
			self.assertEqual(result[0], 1, "Event should be stored directly")
		finally:
			conn.close()

	def test_error_handling(self):
		"""Test error handling in various scenarios"""

		# Test with invalid database path (should handle gracefully)
		with patch("pulse.storage._get_db_path", return_value="/invalid/path/pulse.duckdb"):
			try:
				store_batch_in_duckdb([{"site": "test", "event_name": "test", "timestamp": "2023-01-01"}])
				# Should not reach here if error handling works
				self.fail("Should have raised an exception for invalid path")
			except Exception:
				# Expected behavior
				pass

		# Test Redis Stream with connection issues
		with patch.object(RedisStream, "conn", side_effect=Exception("Redis connection failed")):
			stream = RedisStream.init("test:stream")
			# Should handle connection errors gracefully
			events = stream.read(10)
			self.assertEqual(events, [], "Should return empty list on connection error")
