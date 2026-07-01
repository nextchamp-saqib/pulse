# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

"""Cookieless anonymous identity for stage-1 website visitors.

``anonymous_mode = "cookieless"`` (the default) writes nothing to the browser — no
cookie, no localStorage. The anonymous ``user`` is derived server-side, at ingest,
from data already on the direct browser request:

    user = "anon_" + sha256(daily_salt | site | ip | user_agent)

``daily_salt`` rotates every UTC day and the previous day's value is discarded, so a
visitor is one stable id within a day and a different id the next — cross-day
attribution is intentionally dropped (the Plausible model). Same-session anon→signup
stitching still works: the click-time ``aid`` forward carries this id into ``alias()``
(see ``getDistinctId`` in ``pulse_client.js`` and the ``anon_id`` endpoint).

The salt is global to this Pulse server; ``site`` is in the hashed input, so the same
browser is already a different id per host site without a per-site salt.
"""

import datetime
import hashlib
import secrets

import frappe
from frappe.utils.synchronization import filelock

ANON_PREFIX = "anon_"
ANON_ID_LENGTH = 16

# Per-worker cache of the day's salt, keyed by UTC day. The salt of record lives in
# Pulse Settings; this spares a DB read per derive and misses once per worker per day.
_salt_cache: dict[str, str] = {}


def _utc_day() -> str:
	"""Today's date in UTC as ``YYYY-MM-DD`` — the salt's rotation key."""
	return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")


def daily_salt() -> str:
	"""Return the current UTC day's global salt, rotating it on a day boundary."""
	day = _utc_day()
	cached = _salt_cache.get(day)
	if cached:
		return cached

	salt = _load_or_rotate_salt(day)
	# Drop stale days so the cache holds at most the current one.
	_salt_cache.clear()
	_salt_cache[day] = salt
	return salt


def _load_or_rotate_salt(day: str) -> str:
	salt = frappe.db.get_single_value("Pulse Settings", "anon_salt")
	if salt and frappe.db.get_single_value("Pulse Settings", "anon_salt_day") == day:
		return salt
	return _rotate_salt(day)


def _rotate_salt(day: str) -> str:
	"""Mint and persist a fresh salt for ``day``, discarding the previous one.

	Locked so requests crossing UTC midnight settle on one salt instead of racing.
	"""
	with filelock("pulse_anon_salt", timeout=10):
		# Re-read inside the lock: another worker may have just rotated.
		settings = frappe.get_doc("Pulse Settings")
		if settings.anon_salt and settings.anon_salt_day == day:
			return settings.anon_salt

		salt = secrets.token_hex(32)
		settings.anon_salt = salt
		settings.anon_salt_day = day
		settings.save(ignore_permissions=True)
		frappe.db.commit()
		return salt


def derive_anon_user(site: str | None) -> str:
	"""Derive this request's cookieless ``user`` from salt + site + ip + ua.

	The ``anon_id`` endpoint and the ingest path derive identically, so a visitor's
	forwarded ``aid`` matches the ``user`` stored on its events. ``site`` is required —
	it scopes identity, and a missing one would yield an id no real event matches.
	"""
	if not site:
		frappe.throw("site is required", frappe.ValidationError)
	ip = getattr(frappe.local, "request_ip", None) or ""
	ua = frappe.get_request_header("User-Agent") or ""
	raw = "|".join((daily_salt(), site, ip, ua))
	digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
	return ANON_PREFIX + digest[:ANON_ID_LENGTH]
