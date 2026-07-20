# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

"""The set of event names in use, discovered from the events themselves.

A registry someone has to declare into drifts from the code the moment a developer
ships a name without updating it — and the names most worth catching are exactly the
ones nobody meant to create. So this is derived, not declared: every name that
survives validation records itself here, and the only human act is reviewing what
turned up.

That makes the naming mistakes visible while they are still cheap. `initated_client_side`
ran for 20k events before anyone noticed the typo; `helpdesk:new_ticket_page` and
`new_ticket_page` are two names for one thing. Both show up here within a day.

Nothing in this doctype changes what is captured. Blocking a name is a Pulse Capture
Rule on Event Name — one mechanism for "stop storing this", not two.
"""

import frappe
from frappe.model.document import Document
from frappe.utils import cint, get_datetime

from pulse.logger import get_logger

logger = get_logger()

# An event name reported by many apps is worth seeing, but the list is a hint, not a
# record — cap it so a name fired from hundreds of custom apps can't grow unbounded.
MAX_APPS_TRACKED = 10


class PulseEventCatalog(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		apps: DF.SmallText | None
		description: DF.SmallText | None
		event_count: DF.Int
		event_name: DF.Data
		first_seen: DF.Datetime | None
		last_seen: DF.Datetime | None
		status: DF.Literal["Discovered", "Approved", "Deprecated"]
	# end: auto-generated types

	pass


def record_events(events: list[dict]):
	"""Fold a consumed batch into the catalog.

	Called after the batch is durably stored, and never allowed to fail the drain:
	the catalog is bookkeeping, and losing a note about a name is not worth losing
	the events it describes.

	Counts what was consumed, which is a hair above what was stored — deduplication
	happens in the insert, so a resent event is counted per delivery. The number is
	here to show whether a name is in real use, not to be reconciled against rows.
	"""
	try:
		_record_events(_summarize(events))
		frappe.db.commit()
	except Exception:
		frappe.db.rollback()
		logger.error(
			{
				"message": "Failed to update the event catalog",
				"error": frappe.get_traceback(with_context=True),
			}
		)


def _summarize(events: list[dict]) -> dict:
	"""Reduce a batch to one entry per event name, so the catalog costs a few
	queries per drain rather than a few per event."""
	summary = {}
	for event in events:
		name = event.get("event_name")
		if not name:
			continue
		# The stream hands back strings; the catalog columns read back as datetimes.
		# Normalize here so first/last comparisons aren't between the two.
		seen_at = get_datetime(event["captured_at"]) if event.get("captured_at") else None
		entry = summary.setdefault(name, {"count": 0, "apps": set(), "first": seen_at, "last": seen_at})
		entry["count"] += 1
		if event.get("app"):
			entry["apps"].add(event["app"])
		if seen_at:
			entry["first"] = min(entry["first"] or seen_at, seen_at)
			entry["last"] = max(entry["last"] or seen_at, seen_at)
	return summary


def _record_events(summary: dict):
	if not summary:
		return

	existing = {
		row.name: row
		for row in frappe.get_all(
			"Pulse Event Catalog",
			filters={"name": ("in", list(summary))},
			fields=["name", "event_count", "apps", "first_seen", "last_seen"],
		)
	}

	for event_name, entry in summary.items():
		row = existing.get(event_name)
		if row:
			frappe.db.set_value(
				"Pulse Event Catalog",
				event_name,
				{
					"event_count": cint(row.event_count) + entry["count"],
					"apps": _merge_apps(row.apps, entry["apps"]),
					"first_seen": _pick(min, row.first_seen, entry["first"]),
					"last_seen": _pick(max, row.last_seen, entry["last"]),
				},
				update_modified=False,
			)
		else:
			frappe.get_doc(
				{
					"doctype": "Pulse Event Catalog",
					"event_name": event_name,
					"event_count": entry["count"],
					"apps": _merge_apps(None, entry["apps"]),
					"first_seen": entry["first"],
					"last_seen": entry["last"],
				}
			).insert(ignore_permissions=True, ignore_if_duplicate=True)


def _pick(chooser, stored, incoming):
	"""Apply min/max across values either of which may be missing."""
	present = [value for value in (stored, incoming) if value is not None]
	return chooser(present) if present else None


def _merge_apps(existing: str | None, incoming: set) -> str:
	apps = {app for app in (existing or "").split(", ") if app} | incoming
	return ", ".join(sorted(apps)[:MAX_APPS_TRACKED])
