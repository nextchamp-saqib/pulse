"""Restore Pulse Event creation timestamps clobbered by the warehouse migration.

`migrate_warehouse_to_db` overwrote the creation/modified of every then-existing
Pulse Event with the migration's own run time, breaking the creation-sorted list
view. The run stamped values across an ~8-minute window (one timestamp per batch),
so we match that window rather than a single value.

The original creation was the event's receive time, still recoverable from the
Redis stream entry id (`<unix_ms>-<seq>`) the row is named by: rebuild it from that
prefix and set modified to match. That reconstructed receive time also tells a
clobbered row apart from an event *genuinely* received during the window -- a
clobbered row's true receive time predates the migration, a real one's doesn't --
so we only rewrite rows whose receive time is older than the window. Non-stream
backfill rows (no embedded receive time) fall back to captured_at.

Runs as a batched background job to stay out of the migrate downtime window.
Idempotent: rewritten rows leave the window.
"""

import frappe
from frappe.utils import get_system_timezone

# The migration's run window, in the instance's local time (IST). Clobbered
# creations all fall inside it; the end is padded a little -- the receive-time
# guard below, not this bound, is what keeps genuinely-received events safe.
CLOBBER_START = "2026-06-18 21:00:00"
CLOBBER_END = "2026-06-18 21:10:00"

# IST is a fixed +05:30 offset (no DST), so pinning the session to it lets
# FROM_UNIXTIME rebuild the original naive-local receive time from the stream id.
IST_OFFSET = "+05:30"

BATCH_SIZE = 25000

# Receive time embedded in the stream entry id, as naive IST. DECIMAL (not float)
# division keeps the millisecond precision exact.
_RECV = "FROM_UNIXTIME(CAST(SUBSTRING_INDEX(name, '-', 1) AS DECIMAL(20, 3)) / 1000)"


def execute():
	# The window bounds and +05:30 reconstruction below are specific to the affected
	# IST instance. Refuse to run on any other timezone so a site that happens to have
	# events in this wall-clock window can't be rewritten in the wrong local time.
	if get_system_timezone() not in ("Asia/Kolkata", "Asia/Calcutta"):
		return

	if frappe.db.sql(
		"SELECT 1 FROM `tabPulse Event` WHERE creation >= %s AND creation < %s LIMIT 1",
		(CLOBBER_START, CLOBBER_END),
	):
		frappe.enqueue(
			"pulse.patches.restore_event_creation_timestamps._restore",
			queue="long",
			timeout=14400,
			job_id="pulse_restore_event_creation",
			deduplicate=True,
		)


def _restore():
	frappe.db.sql(f"SET time_zone = '{IST_OFFSET}'")

	# Stream-named rows whose true receive time predates the window were clobbered;
	# rebuild creation from that receive time. Events genuinely received during the
	# window (receive time inside it) are skipped by the guard.
	while True:
		frappe.db.sql(
			f"""
			UPDATE `tabPulse Event`
			SET creation = {_RECV}, modified = creation
			WHERE creation >= %s AND creation < %s
			  AND name REGEXP '^[0-9]+-'
			  AND {_RECV} < %s
			LIMIT {BATCH_SIZE}
			""",
			(CLOBBER_START, CLOBBER_END, CLOBBER_START),
		)
		changed = frappe.db._cursor.rowcount
		frappe.db.commit()
		if not changed:
			break

	# Non-stream backfill rows have no embedded receive time -> captured_at, limited
	# to historical rows (captured before the window) the migration stamped.
	frappe.db.sql(
		"""
		UPDATE `tabPulse Event` SET creation = captured_at, modified = captured_at
		WHERE creation >= %s AND creation < %s
		  AND name NOT REGEXP '^[0-9]+-'
		  AND captured_at IS NOT NULL AND captured_at < %s
		""",
		(CLOBBER_START, CLOBBER_END, CLOBBER_START),
	)
	frappe.db.commit()
