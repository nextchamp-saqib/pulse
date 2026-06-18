"""Helpers for the end-to-end telemetry test (cypress/integration/pulse_telemetry.js).

These exist to make the otherwise-async, multi-hop pipeline deterministic from a
single Cypress run:

    browser capture -> bulk_capture -> client Redis queue
        -> send_queued_events() -> bulk_ingest -> Pulse Redis stream
        -> consume_pulse_events() -> Pulse Event row

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


def setup(api_key: str = "asdf1234"):
	"""Force-enable the framework pulse client on this (dev) site and make the
	Pulse app accept the client's API key, so the full chain runs locally."""
	# Flip the test seam in frappe.utils.telemetry.pulse.client.is_enabled().
	update_site_config("pulse_force_enabled", 1)

	# The client sends `X-Pulse-API-Key: <pulse_api_key>`; the ingest endpoint
	# validates it against Pulse Settings. Keep them in sync.
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
	"""Run both scheduler hops by hand: client queue -> ingest -> stream -> DB."""
	from frappe.utils.telemetry.pulse.client import send_queued_events

	from pulse.pulse.doctype.pulse_event.pulse_event import consume_pulse_events

	send_queued_events()  # HTTP POST to bulk_ingest, lands in the Redis stream
	consume_pulse_events()  # drains the stream into the Pulse Event table
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
