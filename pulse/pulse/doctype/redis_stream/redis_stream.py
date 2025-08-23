# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import os
import re
import time
from contextlib import suppress

import frappe
from frappe.model.document import Document
from frappe.utils import cstr
from frappe.utils.background_jobs import get_redis_conn

from pulse.constants import (
	PENDING_MIN_IDLE_MS,
	STREAM_MAX_LENGTH,
)
from pulse.logger import get_logger
from pulse.utils import decode, pretty_bytes

logger = get_logger()


class RedisStream(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		from pulse.pulse.doctype.redis_stream_consumer.redis_stream_consumer import RedisStreamConsumer
		from pulse.pulse.doctype.redis_stream_entry.redis_stream_entry import RedisStreamEntry

		consumers: DF.Table[RedisStreamConsumer]
		entries: DF.Table[RedisStreamEntry]
		entries_per_minute: DF.Data | None
		lag: DF.Data | None
		length: DF.Int
		memory_usage: DF.Data | None
		title: DF.Data | None
	# end: auto-generated types

	@classmethod
	def init(cls, name) -> "RedisStream":
		return RedisStream(
			doctype="Redis Stream",
			name=name,
		)

	@classmethod
	def connect(cls):
		# using redis queue connection as it has some level of persistence
		return get_redis_conn()

	@property
	def conn(self):
		if not hasattr(self, "_conn"):
			self._conn = self.connect()
		return self._conn

	@property
	def key(self):
		return f"{frappe.local.site}:{self.name}"

	@property
	def group(self):
		return "event_processors"

	@property
	def consumer(self):
		consumer_name = os.environ.get("RQ_WORKER_ID") or "default_worker"
		return consumer_name

	def db_insert(self, *args, **kwargs):
		raise NotImplementedError

	def load_from_db(self):
		self.create_if_not_exists()
		doc = {
			"name": self.name,
			"length": self.get_length(),
			"lag": self.get_unacknowledged_length(),
			"memory_usage": pretty_bytes(self.get_memory_usage()),
			"entries_per_minute": self.get_entries_per_minute(),
			"consumers": self.get_consumers(),
			"entries": self.get_entries(),
		}

		super(Document, self).__init__(doc)

	def create_if_not_exists(self):
		if not self.conn.exists(self.key):
			self.conn.xgroup_create(self.key, self.group, id="0", mkstream=True)

	def get_length(self):
		try:
			length = self.conn.xlen(self.key)
		except Exception as e:
			length = 0
			logger.error(f"Failed to get stream length: {e!s}")

		return length

	def get_info(self):
		try:
			info = self.conn.xinfo_stream(self.key)
			# ensure bytes are converted to strings for JSON serialization
			info = decode(info)
		except Exception:
			info = None

		return info

	def get_unacknowledged_length(self):
		# pending entries that are yet to be acknowledged
		# this will be the count of messages in the "pending" list
		# and the number of messages that have been delivered but not yet acknowledged
		length = 0
		with suppress(Exception):
			for g in self.conn.xinfo_groups(self.key) or []:
				group_name = decode(g.get("name"))
				if group_name == self.group:
					length += int(g.get("pending", 0))
					length += int(g.get("lag", 0))

		return length

	def get_memory_usage(self):
		with suppress(Exception):
			return self.conn.memory_usage(self.key)

	def get_entries_per_minute(self):
		# Calculate the number of entries processed per minute
		# The id of the entries can be used as timestamp to calculate the rate
		interval = 60  # 1 minute
		now_ms = int(time.time() * 1000)
		start_id = f"{now_ms - (interval * 1000)}-0"

		with suppress(Exception):
			items = self.conn.xrange(self.key, min=start_id, max="+")
			return len(items or [])

	def get_group_info(self):
		try:
			group_info = self.conn.xinfo_groups(self.key)
			group_info = decode(group_info)
		except Exception:
			group_info = None

		return group_info

	def get_consumers(self):
		with suppress(Exception):
			consumers = []
			for g in self.conn.xinfo_groups(self.key) or []:
				group = decode(g)
				if group["name"] == self.group:
					for c in self.conn.xinfo_consumers(self.key, group["name"]) or []:
						consumer = decode(c)
						consumer["consumer_name"] = consumer["name"]
						consumer["idle"] = consumer["idle"] / 1000
						consumer["group"] = group["name"]
						consumer["group_info"] = frappe.as_json(group, indent=4)
						consumers.append(consumer)
			return consumers

	def get_entries(self, count=10):
		with suppress(Exception):
			entries = []
			for e in self.conn.xrevrange(self.key, count=count) or []:
				entry = decode(e)
				entries.append(
					{
						"id": entry[0],
						"data": frappe.as_json(entry[1], indent=4),
					}
				)
			return entries

	def db_update(self):
		raise NotImplementedError

	def delete(self):
		with suppress(Exception):
			self.conn.delete(self.key)

	@staticmethod
	def get_list(filters=None, page_length=20, **kwargs):
		conn = RedisStream.connect()
		pattern = f"{frappe.local.site}:*"
		streams = []

		name_pattern = re.compile(pattern.replace("*", "(.*)"))
		for key in conn.scan_iter(match=pattern, type="stream"):
			name_match = name_pattern.match(key.decode("utf-8"))
			if name_match:
				streams.append({"name": name_match.group(1)})
		return streams

	@staticmethod
	def get_count(filters=None, **kwargs):
		pass

	@staticmethod
	def get_stats(**kwargs):
		pass

	def add(self, data):
		try:
			serialized = self.serialize(data)
			self.conn.xadd(self.key, serialized, maxlen=STREAM_MAX_LENGTH, approximate=True)
		except Exception as e:
			logger.error(f"Failed to add entry to Redis stream: {e!s}")
			raise

	def serialize(self, data):
		serialized = {}
		for key, value in data.items():
			if value is not None:
				serialized[key] = cstr(value)
		return serialized

	def ack_entries(self, ids):
		try:
			if not ids:
				return
			self.conn.xack(self.key, self.group, *ids)
		except Exception as e:
			logger.error(f"Failed to acknowledge entries: {e!s}")

	def read_pending(self, count=100):
		result = self.conn.xreadgroup(
			self.group,
			self.consumer,
			{self.key: "0"},
			count=count,
		)
		return self._extract_entries(result)

	def read_stale(self, count=100):
		result = self.conn.xautoclaim(
			self.key,
			self.group,
			self.consumer,
			min_idle_time=PENDING_MIN_IDLE_MS,
			start_id="0-0",
			count=count,
		)
		with suppress(Exception):
			return result[1]
		return []

	def read_new(self, count=100):
		result = self.conn.xreadgroup(
			self.group,
			self.consumer,
			{self.key: ">"},
			count=count,
		)
		return self._extract_entries(result)

	def read(self, count=100):
		entries = []
		try:
			entries = self.read_pending(count) or []
			if len(entries) < count:
				entries += self.read_stale(count - len(entries)) or []
			if len(entries) < count:
				entries += self.read_new(count - len(entries)) or []
		except Exception as e:
			logger.error(f"Failed to read entries: {e!s}")

		return [
			{
				"id": decode(entry[0]),
				"data": decode(entry[1]),
			}
			for entry in entries
		]

	def _extract_entries(self, result):
		with suppress(Exception):
			return result[0][1]
		return []
