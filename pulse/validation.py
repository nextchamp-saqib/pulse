# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

"""Shape checks applied to every event at the door.

Pulse's ingest key is public by design (it ships in the browser), so authentication
says nothing about whether an event is *sane*. These checks are the only thing
standing between the event table and whatever a caller happens to send — which, in
practice, has been translated UI copy used as an event name, whole documents shipped
as properties, and injection probes fired at the endpoint.

Two kinds of response, chosen by whether the caller could plausibly have meant it:

- **Reject** what is a bug at the call site (a malformed name, an unparseable or
  ancient timestamp). Rejection is reported back per event, so it is visible and
  fixable rather than silently absorbed.
- **Repair** what is merely untrustworthy (a clock running ahead, an oversized
  property bag). Dropping a real event over a skewed clock loses more than it saves.
"""

import datetime
import re

import frappe
from frappe.utils import convert_utc_to_system_timezone, get_datetime

from pulse.constants import (
	EVENT_ID_PATTERN,
	EVENT_NAME_PATTERN,
	MAX_CLOCK_SKEW_MINUTES,
	MAX_EVENT_AGE_DAYS,
	MAX_PROPERTIES_LENGTH,
	MAX_PROPERTY_DEPTH,
	MAX_PROPERTY_ITEMS,
	MAX_PROPERTY_VALUE_LENGTH,
)

_event_name_re = re.compile(EVENT_NAME_PATTERN)
_event_id_re = re.compile(EVENT_ID_PATTERN)

MAX_CLOCK_SKEW = datetime.timedelta(minutes=MAX_CLOCK_SKEW_MINUTES)
MAX_EVENT_AGE = datetime.timedelta(days=MAX_EVENT_AGE_DAYS)


def validate_event_name(event_name: str):
	"""Reject a name that no deliberate call site would produce.

	An event name is a stable analytical key, so it has to be a constant in the code
	that fires it. Names assembled at runtime — from a label the user sees, or a
	doctype appended to a literal — mint a new key per variant and are the reason a
	dashboard silently misses rows; they fail here instead.
	"""
	if not _event_name_re.match(event_name or ""):
		frappe.throw(
			f"Invalid event_name {(event_name or '')[:64]!r}: expected lowercase snake_case, "
			"optionally namespaced (e.g. 'ticket_created' or 'helpdesk:ticket_created')",
			frappe.ValidationError,
		)


def validate_event_id(event_id) -> str | None:
	"""Check the client's idempotency key, or return None when it sent none.

	The key backs a unique index, so a malformed one is worth turning away: it would
	either be rejected by the column or, worse, collide with another caller's. An
	absent key is fine — it only means that caller opts out of deduplication.
	"""
	if not event_id:
		return None
	if not _event_id_re.match(str(event_id)):
		frappe.throw(
			f"Invalid event_id {str(event_id)[:64]!r}: expected 8-64 characters of [A-Za-z0-9_-]",
			frappe.ValidationError,
		)
	return event_id


def resolve_captured_at(captured_at, received_at: datetime.datetime) -> datetime.datetime:
	"""Return a `captured_at` worth storing, given the client's clock is not trusted.

	A clock running ahead is clamped to the receive time: `now` is the best estimate
	available, and a future-dated event would otherwise pin itself to the top of every
	recent-events view until the date caught up. A clock far *behind* is rejected
	rather than clamped — relabelling a 1970 timestamp as "now" would invent activity
	that never happened, and quietly, whereas a rejection is reported to the caller.
	"""
	try:
		resolved = get_datetime(captured_at)
	except Exception:
		resolved = None
	if not isinstance(resolved, datetime.datetime):
		frappe.throw(f"Invalid captured_at: {str(captured_at)[:64]!r}", frappe.ValidationError)

	# Normalize to naive system time, which is what the column stores and what
	# `received_at` already is — an aware value would not even be comparable here.
	if resolved.tzinfo is not None:
		resolved = convert_utc_to_system_timezone(resolved).replace(tzinfo=None)

	if resolved > received_at + MAX_CLOCK_SKEW:
		return received_at
	if resolved < received_at - MAX_EVENT_AGE:
		frappe.throw(
			f"captured_at is more than {MAX_EVENT_AGE_DAYS} days old: {resolved}",
			frappe.ValidationError,
		)
	return resolved


def serialize_properties(properties) -> str:
	"""Serialize the property bag to JSON, capped to a size a column can live with.

	Long string values are truncated in place rather than dropped, so an oversized
	payload still yields a usable event — the keys survive, only the overrun is lost.
	A bag that is still too large after that is replaced wholesale by a marker: at
	that point it is a document, not a description, and keeping a fraction of it
	would be more misleading than keeping none.
	"""
	if isinstance(properties, str):
		try:
			properties = frappe.parse_json(properties)
		except Exception:
			properties = None
	if not isinstance(properties, dict):
		properties = {}

	capped = {key: _cap(value) for key, value in properties.items()}
	serialized = frappe.as_json(capped)
	if len(serialized) > MAX_PROPERTIES_LENGTH:
		return frappe.as_json({"_truncated": True, "_length": len(serialized)})
	return serialized


def _cap(value, depth: int = 0):
	if isinstance(value, str):
		return value[:MAX_PROPERTY_VALUE_LENGTH]
	if depth >= MAX_PROPERTY_DEPTH:
		# Deeper than any property bag has reason to be; keep a readable stub.
		return str(value)[:MAX_PROPERTY_VALUE_LENGTH]
	if isinstance(value, dict):
		return {key: _cap(item, depth + 1) for key, item in value.items()}
	if isinstance(value, list | tuple):
		return [_cap(item, depth + 1) for item in value[:MAX_PROPERTY_ITEMS]]
	return value
