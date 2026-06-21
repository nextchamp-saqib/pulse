"""Helpers for the end-to-end telemetry test (cypress/integration/pulse_telemetry.js).

These exist to make the otherwise-async pipeline deterministic from a single
Cypress run. The browser posts directly to pulse (key in the request body):

    browser capture -> POST pulse.api.bulk_ingest -> Pulse Redis stream
        -> consume_pulse_events() -> Pulse Event row

The server-side path (Python `capture()`) still goes through the framework's queue:

    capture() -> framework Redis queue
        -> send_queued_events() -> POST bulk_ingest -> Pulse Redis stream
        -> consume_pulse_events() -> Pulse Event row

`drain()` runs both server hops so either path lands deterministically.

Invoke via `bench --site <site> execute pulse.e2e.<fn>`. Test-only; never wired
into hooks or scheduler.
"""

import frappe
from frappe.installer import update_site_config

E2E_EVENT_NAME = "cypress_pulse_e2e"

# Dedicated login for the e2e run. We provision our own System Manager rather than
# depend on the site's admin password (which isn't in site_config, so run-ui-tests
# can't inject it) — keeps the test self-contained and non-destructive.
E2E_USER = "pulse-e2e@example.com"
E2E_PASSWORD = "Pulse-E2E-passw0rd!"


def setup(api_key: str = "asdf1234", host: str = "http://pulse.localhost:8000"):
	"""Force-enable the pulse client on this (dev) site and make the Pulse app
	accept the client's API key, so the full chain runs locally.

	`host` points the browser client (boot.telemetry.host) at the *local* pulse so
	the direct ingest POST lands here instead of the real pulse.m.frappe.cloud. On
	this single self-hosting site that's same-origin, so no CORS is involved."""
	# Flip the test seam in frappe.utils.telemetry.pulse.client.is_enabled().
	update_site_config("pulse_force_enabled", 1)

	# Point the browser client at the local pulse (host + client asset).
	update_site_config("pulse_host", host)

	# The client sends the key (browsers in the body, server-to-server in the
	# X-Pulse-API-Key header); the ingest endpoint validates it against Pulse
	# Settings. Keep them in sync.
	update_site_config("pulse_api_key", api_key)
	settings = frappe.get_single("Pulse Settings")
	settings.api_key = api_key
	settings.save(ignore_permissions=True)

	_ensure_test_user()

	frappe.db.commit()
	frappe.clear_cache()  # bust the @site_cache on is_enabled()
	print("pulse e2e: enabled")


def _ensure_test_user():
	"""Create (or reset the password of) the e2e login, with System Manager so it
	can read Pulse Event for the final assertion."""
	from frappe.utils.password import update_password

	if frappe.db.exists("User", E2E_USER):
		update_password(E2E_USER, E2E_PASSWORD)
		return

	user = frappe.new_doc("User")
	user.email = E2E_USER
	user.first_name = "Pulse E2E"
	user.send_welcome_email = 0
	user.new_password = E2E_PASSWORD
	user.append_roles("System Manager")
	user.insert(ignore_permissions=True)


def drain():
	"""Force both server hops by hand so the test is deterministic.

	Covers either path: the browser e2e only needs the consume hop (its POST already
	hit the stream), while the server-only check (Python `capture()` into the
	framework queue) needs the send hop first. `send_queued_events()` is a no-op when
	the framework queue is empty, so running both is always safe."""
	from frappe.utils.telemetry.pulse.client import send_queued_events

	from pulse.pulse.doctype.pulse_event.pulse_event import consume_pulse_events

	send_queued_events()  # framework queue -> POST bulk_ingest -> Redis stream
	consume_pulse_events()  # stream -> Pulse Event table
	frappe.db.commit()
	print("pulse e2e: drained")


def count(event_name: str = E2E_EVENT_NAME):
	"""Print the number of persisted Pulse Event rows for an event name."""
	n = frappe.db.count("Pulse Event", {"event_name": event_name})
	print(f"pulse e2e: count={n}")
	return n


def cleanup(event_name: str = E2E_EVENT_NAME):
	"""Remove rows created by the test so reruns stay clean."""
	frappe.db.delete("Pulse Event", {"event_name": event_name})
	frappe.db.commit()
	print("pulse e2e: cleaned")
