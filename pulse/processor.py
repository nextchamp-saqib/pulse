import frappe

from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream

from .constants import STREAM_MAX_LENGTH
from .logger import get_logger
from .storage import store_batch_in_duckdb

logger = get_logger()

READ_COUNT = int(STREAM_MAX_LENGTH / 2)


def process_events(stream_name=None):
	# Use provided stream name, frappe.flags test stream name, or default to production stream
	if not stream_name:
		stream_name = getattr(frappe.flags, "test_stream_name", "pulse:events")

	stream = RedisStream.init(stream_name)
	events = stream.read(READ_COUNT)
	accepted, discarded = sanitize_events(events)

	try:
		store_batch_in_duckdb(accepted)
	except Exception as e:
		logger.error(f"Error storing events: {e!s}")
		stream.move_to_dlq(accepted)

	# ack all events because
	# accepted ones have been stored or moved to DLQ
	# discarded ones should never to be processed
	stream.ack_entries(events)
	logger.info(f"Processed {len(accepted)} events successfully. Discarded {len(discarded)} events.")


def sanitize_events(events):
	sanitized = []
	discarded = []
	for event in events:
		if not isinstance(event, dict):
			logger.debug(f"Skipping event with invalid data format: {event}")
			discarded.append(event)
			continue

		reqd_fields = ["id", "site", "name", "timestamp"]
		missing = [field for field in reqd_fields if not event.get(field)]
		if missing:
			logger.debug(f"Skipping event with missing required fields: {missing}")
			discarded.append(event)
			continue

		table_fields = [
			"id",
			"site",
			"name",
			"app",
			"app_version",
			"frappe_version",
			"timestamp",
		]

		sanitized_event = {}
		for field in table_fields:
			sanitized_event[field] = event.get(field)

		# Move all extra fields into the data dictionary
		sanitized_event["data"] = {k: v for k, v in event.items() if k not in table_fields}
		sanitized.append(sanitized_event)

	return sanitized, discarded
