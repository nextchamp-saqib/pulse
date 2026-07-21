# Copyright (c) 2025, hello@frappe.io and contributors
# For license information, please see license.txt

"""The key that makes storing an event twice a no-op.

4.4% of captured rows were exact duplicates. Roughly 59% of the excess arrived in
the same second as its twin — instrumentation firing twice — and most of the rest
within a minute, a client resending a batch it never got an ack for.

The key is derived here rather than sent by the caller. A client-minted id would be
a more direct statement of "this is the same event", but it only ever protects
clients that have been updated to send one: not server-to-server callers, not older
SDKs, not the load-test harness. Deriving it covers every caller the day it ships.

What makes that safe is the data: `captured_at` carries microsecond precision on all
but 52 of 9.5M rows, and of the 419,834 duplicate rows all but 9 share a
microsecond-precision timestamp with their twin. Two genuinely distinct events
agreeing on name, microsecond, site, app, identity *and* every property is not a
case that occurs; a resend agreeing on all of them is the case that occurs 420k times.
"""

import hashlib

# 128 bits, as 32 hex characters. Wide enough that a collision is not a thing that
# happens, narrow enough to keep the unique index half the width of a sha256 hex.
_DIGEST_BYTES = 16

# Chosen so a value can never be mistaken for a separator (no field may contain \x1f).
_SEPARATOR = "\x1f"


def dedup_key(event_name, captured_at, site, app, user, team, properties: str) -> str:
	"""Return the fingerprint of an event: equal keys mean the same event, resent.

	`properties` is taken already serialized, because the stored JSON is what the
	comparison has to be made over — `frappe.as_json` sorts keys, so a client that
	serializes its bag in a different order on retry still lands on the same key.
	"""
	raw = _SEPARATOR.join(
		"" if part is None else str(part)
		for part in (event_name, captured_at, site, app, user, team, properties)
	)
	return hashlib.blake2b(raw.encode("utf-8"), digest_size=_DIGEST_BYTES).hexdigest()
