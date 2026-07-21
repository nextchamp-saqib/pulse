# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

"""A running count of what ingest did with the events it was sent.

Ingest can now decline an event in two ways — a capture rule drops it, or validation
rejects it — and both are invisible in the stored data, because the whole point is
that nothing gets stored. A rule one character too broad, or an SDK that starts
sending a malformed name, would silently cost traffic that nobody would think to
look for. These counters are what make that noticeable.

They also answer the question the event table cannot: *was anything sent at all?*
A three-day dip in June 2026 (~9k/day against a ~40k baseline) is plain in the
captured data after the fact, and went unnoticed while it happened.

Counting is best-effort and never on the critical path — a batch is counted once,
after its events are staged, and a Redis hiccup costs a number, not an event.
"""

import frappe
from frappe.utils import getdate, now_datetime

ACCEPTED = "accepted"
DROPPED = "dropped"
REJECTED = "rejected"
OUTCOMES = (ACCEPTED, DROPPED, REJECTED)

# Long enough to see a trend and to notice a dip after a weekend, short enough that
# the keys stay a rounding error in Redis.
RETENTION_DAYS = 90

_KEY_PREFIX = "pulse:ingest"


def _key(day: str, outcome: str) -> str:
	# Namespaced by hand: the counters are plain integers, so they use raw Redis
	# commands rather than the cache helpers, which pickle what they store.
	return frappe.cache.make_key(f"{_KEY_PREFIX}:{day}:{outcome}")


def record(accepted: int = 0, dropped: int = 0, rejected: int = 0):
	"""Add a batch's outcomes to today's counters."""
	counts = {ACCEPTED: accepted, DROPPED: dropped, REJECTED: rejected}
	day = str(getdate(now_datetime()))
	try:
		for outcome, count in counts.items():
			if not count:
				continue
			key = _key(day, outcome)
			frappe.cache.incrby(key, count)
			# Refreshed on every write, so a key outlives the last day it was touched
			# rather than expiring mid-day.
			frappe.cache.expire(key, RETENTION_DAYS * 24 * 60 * 60)
	except Exception:
		# Losing a count is not worth failing an ingest that already succeeded.
		pass


def read(days: int = 30) -> list[dict]:
	"""Return per-day counts, most recent first, for the last `days` days."""
	today = getdate(now_datetime())
	wanted = [str(frappe.utils.add_days(today, -offset)) for offset in range(days)]

	keys = [_key(day, outcome) for day in wanted for outcome in OUTCOMES]
	try:
		values = frappe.cache.mget(keys)
	except Exception:
		return []

	rows = []
	for index, day in enumerate(wanted):
		counts = {
			outcome: int(values[index * len(OUTCOMES) + position] or 0)
			for position, outcome in enumerate(OUTCOMES)
		}
		if any(counts.values()):
			rows.append({"day": day, **counts, "total": sum(counts.values())})
	return rows
