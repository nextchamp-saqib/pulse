import time
from contextlib import closing
from datetime import datetime, timezone

import frappe

from pulse.storage import _get_db, get_db_size

from ..stream import RedisStream

# Constants for clarity
INTERVAL = 60 * 10
MAX_SAMPLE_ROWS = 2000


def _utc_now_iso() -> str:
	return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _events_received_last_hour(stream: RedisStream) -> int:
	"""Count events appended to Redis Stream in the last 60 minutes."""
	try:
		now_ms = int(time.time() * 1000)
		start_id = f"{now_ms - (INTERVAL * 1000)}-0"
		# '+' means maximum possible ID
		items = stream.client.xrange(stream.name, min=start_id, max="+")
		return len(items or [])
	except Exception:
		return 0


def _events_processed_last_hour() -> int:
	"""Count events persisted to DuckDB in the last 60 minutes."""
	try:
		with closing(_get_db()) as conn:
			count = conn.execute(
				"""
				SELECT COUNT(*)
				FROM event
				WHERE timestamp >= now() - INTERVAL '10 minutes'
				"""
			).fetchone()[0]
			return int(count or 0)
	except Exception:
		return 0


def _processing_lag_seconds() -> int:
	"""Average processing lag (seconds) for events stored in the last hour."""
	try:
		with closing(_get_db()) as conn:
			rows = conn.execute(
				f"""
				SELECT event_id, timestamp
				FROM event
				WHERE timestamp >= now() - INTERVAL '10 minutes'
				LIMIT {MAX_SAMPLE_ROWS}
				"""
			).fetchall()

		if not rows:
			return 0

		def parse_receipt_ms(event_id):
			try:
				return int(str(event_id).split("-", 1)[0])
			except Exception:
				return None

		def ensure_utc(dt):
			try:
				return dt if (dt and dt.tzinfo) else (dt.replace(tzinfo=timezone.utc) if dt else None)
			except Exception:
				return None

		lags = []
		for event_id, timestamp in rows:
			receipt_ms = parse_receipt_ms(event_id)
			created_dt = ensure_utc(timestamp)
			if receipt_ms is None or not created_dt:
				continue

			receipt_dt = datetime.fromtimestamp(receipt_ms / 1000.0, tz=timezone.utc)
			delta = (created_dt - receipt_dt).total_seconds()
			if delta >= 0:
				lags.append(delta)

		if not lags:
			return 0
		return int(sum(lags) / len(lags))
	except Exception:
		return 0


def _events_in_stream(stream: RedisStream) -> int:
	try:
		return int(stream.length())
	except Exception:
		return 0


def _events_pending(stream: RedisStream) -> int:
	try:
		return int(stream.unacknowledged_length())
	except Exception:
		return 0


def _stream_memory_bytes(stream: RedisStream) -> int:
	"""Return Redis memory usage for the stream key if available."""
	try:
		usage = stream.client.memory_usage(stream.name)
		return int(usage or 0)
	except Exception:
		return 0


@frappe.whitelist()
def get_stats():
	"""Compute and return live ingestion/processing stats."""
	stream = RedisStream()

	received = _events_received_last_hour(stream)
	processed = _events_processed_last_hour()
	processing_rate = round(processed / received, 4) if received else 0.0

	return {
		# ingestion stats
		"events_received_per_hour": received,
		"events_processed_per_hour": processed,
		"processing_rate": processing_rate,
		"processing_lag_seconds": _processing_lag_seconds(),
		"events_in_stream": _events_in_stream(stream),
		"events_pending": _events_pending(stream),
		# resource usage
		"stream_memory_bytes": _stream_memory_bytes(stream),
		"duckdb_size_bytes": get_db_size(),
		"last_updated": _utc_now_iso(),
	}
