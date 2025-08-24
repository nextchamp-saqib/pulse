import frappe

from pulse.pulse.doctype.pulse_event.pulse_event import PulseEvent
from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream

from .constants import STREAM_MAX_LENGTH
from .logger import get_logger
from .storage import store_batch_in_duckdb

logger = get_logger()

READ_COUNT = int(STREAM_MAX_LENGTH / 2)


def process_events():
	stream = RedisStream.init()
	entries = stream.read(READ_COUNT)
	events = [PulseEvent._from_stream_entry(entry) for entry in entries]
	accepted, discarded = sanitize_events(events)

	failed = []
	try:
		store_batch_in_duckdb(accepted)
	except Exception as e:
		logger.error(f"Error storing events: {e!s}")
		failed.extend(accepted)
		stream.move_to_dlq(failed)

	# ack all events because
	# accepted ones have been stored or moved to DLQ
	# discarded ones should never to be processed
	stream.ack_entries(entries)
	logger.info(
		f"Processed {len(accepted)} events. Discarded {len(discarded)} events. Failed {len(failed)} events."
	)


def sanitize_events(events):
	sanitized = []
	discarded = []
	for event in events:
		reqd_fields = ["name", "site", "event_name", "timestamp"]
		missing = [field for field in reqd_fields if not event.get(field)]
		if missing:
			logger.debug(f"Skipping event with missing required fields: {missing}")
			discarded.append(event)
			continue
		sanitized.append(event)

	return sanitized, discarded
