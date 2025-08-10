import frappe

from .constants import STREAM_MAX_LENGTH
from .logger import get_logger
from .storage import store_batch_in_duckdb
from .stream import RedisStream

logger = get_logger()


class EventProcessor:
	def __init__(self):
		self.stream = RedisStream()

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
			event_list = self.stream.read(count=STREAM_MAX_LENGTH / 2, from_consumer_group=True)

			if not event_list:
				return 0

			processed_event_ids = []
			failed_event_ids = []

			batch_logs = []
			batch_log_ids = []
			skipped_ids = []

			for event in event_list:
				try:
					log_data = self._prepare_event(event)
					if log_data is None:
						# Event skipped due to missing mandatory fields; treat as processed for ack
						skipped_ids.append(event.get("id"))
						continue
					batch_logs.append(log_data)
					batch_log_ids.append(event.get("id"))
				except Exception as e:
					logger.error(f"Failed to prepare event: {e!s}")
					failed_event_ids.append(event.get("id"))

			# Write the valid events in a single batch
			if batch_logs:
				try:
					store_batch_in_duckdb(batch_logs)
					processed_event_ids.extend(batch_log_ids)
				except Exception as e:
					logger.error(f"Failed to store event batch in DuckDB: {e!s}")
					failed_event_ids.extend(batch_log_ids)

			# Add skipped ids (considered processed for ack purposes)
			if skipped_ids:
				processed_event_ids.extend(skipped_ids)

			# Only acknowledge successfully processed events
			if processed_event_ids:
				self.stream.acknowledge_events(processed_event_ids)

			# Log metrics for RQ job monitoring
			if processed_event_ids or failed_event_ids:
				logger.info(
					f"Processed {len(processed_event_ids)} events, failed {len(failed_event_ids)} events"
				)

			return len(processed_event_ids)

		except Exception as e:
			logger.error(f"Error processing events: {e!s}")
			return 0  # Return 0 instead of raising to avoid RQ retry

	def _prepare_event(self, event):
		event_data = event.get("data", {})
		log_data = {
			"event_id": event.get("id"),
			"site_name": event_data.get("site_name"),
			"event_name": event_data.get("event"),
			"app_name": event_data.get("app_name"),
			"app_version": event_data.get("app_version"),
			"timestamp": event_data.get("timestamp"),
			"additional_data": {
				k: v
				for k, v in event_data.items()
				if k
				not in [
					"site_name",
					"event",
					"app_name",
					"app_version",
					"timestamp",
				]
			},
		}

		if not log_data["site_name"] or not log_data["event_name"]:
			logger.debug(f"Skipping event with missing site_name or event_name: {log_data}")
			return None

		return log_data


def process_events():
	processor = EventProcessor()
	processed_count = processor.process()
	frappe.debug_log = [f"Processed {processed_count} events"]
