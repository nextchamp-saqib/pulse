import frappe

from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream

from .constants import STREAM_MAX_LENGTH
from .logger import get_logger
from .storage import store_batch_in_duckdb

logger = get_logger()

READ_COUNT = int(STREAM_MAX_LENGTH / 2)


class EventProcessor:
	def __init__(self):
		self.stream = RedisStream.init("pulse:events")

	def _warn_if_stream_near_capacity(self):
		try:
			length = self.stream.length()
			if STREAM_MAX_LENGTH and length and length > 0.8 * STREAM_MAX_LENGTH:
				logger.warning(
					f"Stream at {length}/{STREAM_MAX_LENGTH} (~{int(100 * length / STREAM_MAX_LENGTH)}%). Consider scaling processors or increasing STREAM_MAX_LENGTH."
				)
		except Exception:
			pass

	def process(self):
		try:
			events = self._read_events()

			if not events:
				return 0

			batch, batch_ids, skipped_ids, failed_ids = self._build_batch(events)

			processed_ids, batch_failed_ids = self._store_batch(batch, batch_ids)
			failed_ids.extend(batch_failed_ids)

			# Add skipped ids (considered processed for ack purposes)
			if skipped_ids:
				processed_ids.extend(skipped_ids)

			# Only acknowledge successfully processed events
			if processed_ids:
				self.stream.ack_entries(processed_ids)

			# Log metrics for RQ job monitoring
			if processed_ids or failed_ids:
				logger.info(f"Processed {len(processed_ids)} events, failed {len(failed_ids)} events")

			return len(processed_ids)

		except Exception as e:
			logger.error(f"Error processing events: {e!s}")
			return 0  # Return 0 instead of raising to avoid RQ retry

	def _read_events(self):
		return self.stream.read(READ_COUNT)

	def _build_batch(self, events):
		batch = []
		batch_ids = []
		skipped_ids = []
		failed_ids = []

		for event in events:
			try:
				entry = self._prepare_event(event)
				if entry is None:
					# Event skipped due to missing mandatory fields; treat as processed for ack
					skipped_ids.append(event.get("id"))
					continue
				batch.append(entry)
				batch_ids.append(event.get("id"))
			except Exception as entry:
				logger.error(f"Failed to prepare event: {entry!s}")
				failed_ids.append(event.get("id"))

		return batch, batch_ids, skipped_ids, failed_ids

	def _store_batch(self, batch, batch_ids):
		processed_ids = []
		failed_ids = []

		if not batch:
			return processed_ids, failed_ids

		try:
			store_batch_in_duckdb(batch)
			processed_ids.extend(batch_ids)
		except Exception as e:
			logger.error(f"Failed to store event batch in DuckDB: {e!s}")
			failed_ids.extend(batch_ids)

		return processed_ids, failed_ids

	def _prepare_event(self, event):
		data = event.get("data", {})
		_event = {
			"id": event.get("id"),
			"site": data.get("site"),
			"name": data.get("name"),
			"app": data.get("app"),
			"app_version": data.get("app_version"),
			"frappe_version": data.get("frappe_version"),
			"created_at": data.get("timestamp"),
			"data": {
				k: v
				for k, v in data.items()
				if k
				not in [
					"site",
					"name",
					"app",
					"app_version",
					"frappe_version",
					"created_at",
				]
			},
		}

		if not _event["site"] or not _event["name"]:
			logger.debug(f"Skipping event with missing site or event name: {_event}")
			return None

		return _event


def process_events():
	processor = EventProcessor()
	processed_count = processor.process()
	frappe.debug_log = [f"Processed {processed_count} events"]
