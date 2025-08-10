
import frappe
import redis
from frappe.utils import cstr

from .constants import (
	CONSUMER_GROUP_EXISTS_ERROR,
	CONSUMER_GROUP_NAME,
	PENDING_MIN_IDLE_MS,
	STREAM_MAX_LENGTH,
)
from .logger import get_logger
from .utils import decode

logger = get_logger()


class RedisStream:
	def __init__(self):
		self.name = f"pulse:events:{frappe.local.site}"
		self._client = None
		self.consumer = "processor"

	@property
	def client(self):
		if self._client is None:
			self._client = redis.Redis.from_url(frappe.conf.get("redis_queue"))
		return self._client

	def add(self, stream_data, raise_on_error=True):
		try:
			data = {}
			for key, value in stream_data.items():
				if value is not None:
					data[key] = cstr(value)

			self.client.xadd(self.name, data, maxlen=STREAM_MAX_LENGTH, approximate=True)
		except redis.ConnectionError as e:
			logger.error(f"Redis connection failed for event: {e!s}")
			if raise_on_error:
				raise
		except Exception as e:
			logger.error(f"Failed to add event to Redis stream: {e!s}")
			if raise_on_error:
				raise

	def length(self):
		try:
			return self.client.xlen(self.name)
		except Exception:
			return 0

	def _extract_entries(self, result):
		"""Normalize Redis XREAD/XREADGROUP responses to a flat list of (id, fields)."""
		if not result:
			return []
		try:
			# [(stream, [(id, fields), ...])]
			return result[0][1]
		except Exception:
			logger.debug(f"Failed to extract entries from result: {result!s}")
			return []

	def _read_from_stream(self, count=500):
		result = self.client.xread({self.name: "0"}, count=count)
		return self._extract_entries(result)

	def _read_pending(self, count=500):
		"""Read messages pending for this consumer using id='0'."""
		return self._extract_entries(
			self.client.xreadgroup(
				CONSUMER_GROUP_NAME,
				self.consumer,
				{self.name: "0"},
				count=count,
			)
		)

	def _read_stale(self, count=500):
		"""Claim stale pending entries from other consumers. Returns list[(id, fields)]."""
		try:
			result = self.client.xautoclaim(
				self.name,
				CONSUMER_GROUP_NAME,
				self.consumer,
				min_idle_time=PENDING_MIN_IDLE_MS,
				start_id="0-0",
				count=count,
			)
			claimed = []
			if result and isinstance(result, list | tuple) and len(result) >= 2:
				claimed = result[1]
			return claimed
		except Exception as e:
			logger.debug(f"Failed to claim stale pending entries: {e!s}")
			return []

	def _read_new(self, count=500):
		"""Read new entries (>) for this consumer group."""
		return self._extract_entries(
			self.client.xreadgroup(
				CONSUMER_GROUP_NAME,
				self.consumer,
				{self.name: ">"},
				count=count,
			)
		)

	def _read_from_consumer_group(self, count=500):
		return self._read_pending(count) or self._read_stale(count) or self._read_new(count)

	def read(self, count=500, from_consumer_group=False):
		"""Read events, optionally through a consumer group.

		Returns a list of dicts: [{"id": str, "data": dict}, ...]
		"""
		try:
			raw = []
			if from_consumer_group:
				raw = self._read_from_consumer_group(count)
			else:
				raw = self._read_from_stream(count)
			return self._decode_events(raw)
		except Exception as e:
			logger.debug(
				f"Failed to read from {'consumer group' if from_consumer_group else 'stream'}: {e!s}"
			)
			return []

	def create_consumer_group(self):
		if self.consumer_exists(CONSUMER_GROUP_NAME):
			return

		try:
			self.client.xgroup_create(self.name, CONSUMER_GROUP_NAME, id="0", mkstream=True)
		except redis.exceptions.ResponseError as e:
			if CONSUMER_GROUP_EXISTS_ERROR not in str(e):
				raise

	def consumer_exists(self, group_name):
		try:
			groups = self.client.xinfo_groups(self.name) or []
			for g in groups:
				if decode(g.get("name")) == group_name:
					return True
			return False
		except Exception:
			return False

	def _decode_events(self, events):
		if not events:
			return []

		event_list = []

		for event_id, event_data in events:
			decoded_id = decode(event_id)
			decoded_data = decode(event_data)
			event_list.append({"id": decoded_id, "data": decoded_data})

		return event_list

	def acknowledge_events(self, event_ids):
		try:
			if not event_ids:
				return
			self.client.xack(self.name, CONSUMER_GROUP_NAME, *event_ids)
		except Exception as e:
			logger.error(f"Failed to acknowledge events: {e!s}")

	def pending_length(self) -> int:
		"""Return count of delivered-but-unacknowledged messages (PEL size)."""
		try:
			info = self.client.xpending(self.name, CONSUMER_GROUP_NAME)
			return int(info.get("pending", 0))
		except Exception:
			return 0

	def unread_length(self) -> int:
		"""Return count of messages not yet delivered to the consumer group (lag)."""
		try:
			for g in self.client.xinfo_groups(self.name) or []:
				name = decode(g.get("name"))
				if name == CONSUMER_GROUP_NAME:
					return int(g.get("lag", 0))
			return 0
		except Exception:
			return 0

	def unacknowledged_length(self) -> int:
		"""Total backlog not yet acknowledged for this group: unread + pending."""
		try:
			return int(self.unread_length()) + int(self.pending_length())
		except Exception:
			return 0
