import frappe
import redis
from frappe.utils import cstr

from .constants import (
	CONSUMER_GROUP_EXISTS_ERROR,
	CONSUMER_GROUP_NAME,
	STREAM_MAX_LENGTH,
)
from .logger import get_logger
from .utils import decode

logger = get_logger()


class RedisStream:
	def __init__(self):
		self.name = f"{frappe.local.conf.get('db_name')}:pulse:events"
		self._client = None

	@property
	def client(self):
		if self._client is None:
			self._client = redis.Redis.from_url(frappe.conf.get("redis_cache"))
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

	def read(self, count=500, block=None, from_consumer_group=False):
		try:
			if from_consumer_group:
				self.create_consumer_group()
				events = self.client.xreadgroup(
					CONSUMER_GROUP_NAME,
					f"processor_{frappe.generate_hash(4)}",
					{self.name: ">"},
					count=count,
					block=block,
				)
			else:
				events = self.client.xread({self.name: "0"}, count=count)

			if not events:
				return []

			events = events[0][1]
			return self._decode_events(events)

		except Exception as e:
			error_msg = f"Failed to read from {'consumer group' if from_consumer_group else 'stream'}: {e!s}"
			logger.debug(error_msg)
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
		event_list = []

		for event_id, event_data in events:
			decoded_id = decode(event_id)
			decoded_data = decode(event_data)
			event_list.append({"id": decoded_id, "data": decoded_data})

		return event_list

	def acknowledge_events(self, event_ids):
		try:
			for event_id in event_ids:
				self.client.xack(self.name, CONSUMER_GROUP_NAME, event_id)
		except Exception as e:
			logger.debug(f"Failed to acknowledge events: {e!s}")

	def pending_length(self) -> int:
		"""Return count of delivered-but-unacknowledged messages (PEL size)."""
		try:
			self.create_consumer_group()
			info = self.client.xpending(self.name, CONSUMER_GROUP_NAME)
			return int(info.get("pending", 0))
		except Exception:
			return 0

	def unread_length(self) -> int:
		"""Return count of messages not yet delivered to the consumer group (lag)."""
		try:
			self.create_consumer_group()
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
