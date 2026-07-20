# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

"""Which events are worth keeping, decided by rule rather than by patch.

Not every event that is valid is wanted. A load-test site accounts for 5.6% of all
captured rows; test, staging and demo sites add more. They are indistinguishable
from real traffic by shape — only someone who knows the deployment can say they are
noise — so this is a knob for that person rather than a check in the ingest code.

A rule either drops the event outright or lets it through flagged as internal, which
keeps it available for debugging while leaving it out of reporting. Rules are read on
every ingest, so they are cached and the cache is cleared whenever one changes.
"""

from fnmatch import fnmatchcase

import frappe

DROP = "Drop"
MARK_INTERNAL = "Mark Internal"

_RULE_CACHE_KEY = "pulse_capture_rules"

# Rule's `match_field` -> the event key it is compared against.
_MATCH_FIELDS = {
	"Site": "site",
	"App": "app",
	"Event Name": "event_name",
	"User": "user",
}


def get_rules() -> list[dict]:
	return frappe.cache.get_value(_RULE_CACHE_KEY, generator=_load_rules)


def clear_rule_cache():
	frappe.cache.delete_value(_RULE_CACHE_KEY)


def _load_rules() -> list[dict]:
	return frappe.get_all(
		"Pulse Capture Rule",
		filters={"disabled": 0},
		fields=["match_field", "match_operator", "match_value", "action"],
	)


def evaluate(event_name=None, site=None, app=None, user=None) -> str | None:
	"""Return the action the first matching rule dictates, or None to capture as-is.

	Drop wins over Mark Internal wherever both match: the stricter outcome is the one
	the operator can reason about, and a dropped event can't be un-dropped downstream.
	"""
	event = {"event_name": event_name, "site": site, "app": app, "user": user}
	action = None
	for rule in get_rules():
		if not _matches(rule, event):
			continue
		if rule["action"] == DROP:
			return DROP
		action = rule["action"]
	return action


def _matches(rule: dict, event: dict) -> bool:
	key = _MATCH_FIELDS.get(rule["match_field"])
	value = event.get(key) if key else None
	if not value:
		# A rule targets a value; an event that has none simply isn't what it describes.
		return False
	if rule["match_operator"] == "Matches":
		return fnmatchcase(value, rule["match_value"])
	return value == rule["match_value"]
