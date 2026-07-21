# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

import time

import frappe
from frappe.model.document import Document
from frappe.utils import cint, now_datetime
from frappe.utils.synchronization import LockTimeoutError, filelock

from pulse.capture import DROP, MARK_INTERNAL, evaluate
from pulse.dedup import dedup_key
from pulse.logger import get_logger
from pulse.pulse.doctype.pulse_event_catalog.pulse_event_catalog import record_events
from pulse.pulse.doctype.redis_stream.redis_stream import RedisStream
from pulse.utils import log_error
from pulse.validation import resolve_captured_at, serialize_properties, validate_event_name

logger = get_logger()


_EVENT_STREAMS = {}


def _get_event_stream() -> RedisStream:
	# In tests, a unique stream name is injected per test via
	# frappe.flags.test_stream_name; bypass the cache so each test gets its
	# own stream instead of a stale one from an earlier test.
	if frappe.flags.test_stream_name:
		return RedisStream.init()

	site = getattr(frappe.local, "site", None) or "default"
	# Cache per-site to avoid returning the stream for another site
	if site not in _EVENT_STREAMS:
		_EVENT_STREAMS[site] = RedisStream.init()
	return _EVENT_STREAMS[site]


REQD_FIELDS = ["event_name", "captured_at"]

# Columns written by the consumer for each event row. `name` is the Redis stream
# entry id, which makes the insert idempotent: a redelivered entry collides on the
# primary key and is skipped (see `consume_pulse_events`). `dedup_key` extends that
# same protection out past the host, to an event sent twice. The receive time is not
# stored as its own column — it is the row's `creation`.
_INSERT_FIELDS = [
	"name",
	"dedup_key",
	"event_name",
	"captured_at",
	"site",
	"app",
	"user",
	"team",
	"is_internal",
	"properties",
	"creation",
	"modified",
	"owner",
	"modified_by",
]

# How many entries to pull from the stream per batch, and how long a single
# scheduled drain is allowed to run before yielding to the next tick.
CONSUME_BATCH_SIZE = 1000
CONSUME_TIME_BUDGET_SECONDS = 50


class PulseEvent(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		app: DF.Data | None
		captured_at: DF.Datetime | None
		event_name: DF.Data | None
		properties: DF.JSON | None
		site: DF.Data | None
		team: DF.Data | None
		user: DF.Data | None
	# end: auto-generated types

	def validate(self):
		missing = [field for field in REQD_FIELDS if not getattr(self, field)]
		if missing:
			frappe.throw(f"Missing required fields: {', '.join(missing)}")


def enqueue_event(event_name, captured_at, site=None, app=None, user=None, team=None, properties=None):
	"""Validate and push a single event onto the Redis staging stream.

	This is the single funnel every event passes through, whichever endpoint it
	arrived at, so it is where the shape checks in `pulse.validation` are applied.

	Events are never written to the database synchronously on the ingest path —
	they are buffered in Redis and flushed in batches by `consume_pulse_events`.
	This keeps ingest cheap and absorbs bursts without overrunning the database.

	Returns whether the event was staged; a capture rule may drop it (see
	`pulse.capture`), which is not a failure and is not reported as one.
	"""
	missing = [
		field for field, value in (("event_name", event_name), ("captured_at", captured_at)) if not value
	]
	if missing:
		frappe.throw(f"Missing required fields: {', '.join(missing)}")

	validate_event_name(event_name)

	action = evaluate(event_name=event_name, site=site, app=app, user=user)
	if action == DROP:
		return False

	received_at = now_datetime()
	captured_at = resolve_captured_at(captured_at, received_at)
	# Serialize to JSON here so the value lands in the JSON column as valid JSON
	# (the stream stringifies every field with cstr).
	properties = serialize_properties(properties)

	_get_event_stream().add(
		{
			"dedup_key": dedup_key(event_name, captured_at, site, app, user, team, properties),
			"event_name": event_name,
			"captured_at": captured_at,
			"site": site,
			"user": user,
			"team": team,
			"app": app,
			"is_internal": 1 if action == MARK_INTERNAL else 0,
			"properties": properties,
			"received_at": received_at,
		}
	)
	return True


def _row_from_entry(entry, fallback_ts):
	data = entry.get("data", {})
	# `creation` is the receive time (set at ingest), not the flush time — the row
	# represents the moment the event was received.
	audit_ts = data.get("received_at") or fallback_ts
	return (
		entry.get("id"),  # name == stream entry id (idempotency key)
		# NULL rather than "" for an entry staged before this column existed: the
		# unique index tolerates any number of NULLs but only one "".
		data.get("dedup_key") or None,
		data.get("event_name"),
		data.get("captured_at"),
		data.get("site"),
		data.get("app"),
		data.get("user"),
		data.get("team"),
		cint(data.get("is_internal")),
		data.get("properties") or "{}",
		audit_ts,  # creation
		audit_ts,  # modified
		"Administrator",  # owner
		"Administrator",  # modified_by
	)


@log_error()
def consume_pulse_events():
	"""Drain the Redis staging stream into the `Pulse Event` table.

	Runs on the scheduler. Loops reading batches and bulk-inserting them until the
	stream is empty or the time budget is hit, so a backlog catches up across ticks
	instead of bleeding off one batch per minute. Inserts use `ignore_duplicates`
	keyed on the stream entry id, so the at-least-once delivery of Redis consumer
	groups (pending/stale redelivery) never produces duplicate rows.
	"""
	lock_name = f"pulse_consume_{getattr(frappe.local, 'site', None) or 'default'}"
	try:
		with filelock(lock_name, timeout=1):
			_consume_locked()
	except LockTimeoutError:
		# Another drain is already running; this tick is a no-op.
		return


def _consume_locked():
	stream = _get_event_stream()
	deadline = time.monotonic() + CONSUME_TIME_BUDGET_SECONDS

	while time.monotonic() < deadline:
		entries = stream.read(count=CONSUME_BATCH_SIZE)
		if not entries:
			break

		fallback_ts = now_datetime()
		rows = [_row_from_entry(entry, fallback_ts) for entry in entries]
		frappe.db.bulk_insert("Pulse Event", _INSERT_FIELDS, rows, ignore_duplicates=True)
		frappe.db.commit()

		# Ack only after the rows are durably committed. If we crash before this,
		# the entries stay pending and get redelivered, but the idempotent insert
		# makes the redelivery harmless.
		stream.ack_entries([entry["id"] for entry in entries])

		# Bookkeeping, deliberately last: it must not stand between an event and
		# being stored, and it is allowed to fail on its own.
		record_events([entry.get("data", {}) for entry in entries])

		if len(entries) < CONSUME_BATCH_SIZE:
			break
