import frappe
from frappe.rate_limiter import rate_limit

from pulse.logger import get_logger
from pulse.pulse.doctype.pulse_event.pulse_event import enqueue_event

logger = get_logger()


def get_rate_limit():
	# Max ingest requests allowed per minute, per site (see `key="site"` below).
	# Keyed on site rather than IP: many Frappe Cloud sites share one outbound IP,
	# so a single shared IP bucket would throttle them all.
	return frappe.get_single_value("Pulse Settings", "rate_limit") or 60


@frappe.whitelist(allow_guest=True, methods=["POST"])
@rate_limit(key="site", limit=get_rate_limit, seconds=60)
def ingest(event_name, captured_at, site=None, app=None, user=None, team=None, properties=None):
	check_auth()

	try:
		enqueue_event(
			event_name=event_name,
			captured_at=captured_at,
			site=site,
			app=app,
			user=user,
			team=team,
			properties=properties,
		)
	except Exception as e:
		logger.error(
			{
				"request_ip": frappe.local.request_ip,
				"event": {"event_name": event_name, "site": site, "app": app, "user": user},
				"error": str(e),
			}
		)
		raise e


@frappe.whitelist(allow_guest=True, methods=["POST"])
def bulk_ingest(events, site=None):
	if not isinstance(events, list):
		frappe.throw("Events must be a list", frappe.ValidationError)

	# The rate limiter reads its bucket key from form_dict. Prefer a top-level
	# `site` sent by the client; until the client sends it, fall back to the batch
	# (single-site, since the client's event queue is site-namespaced).
	frappe.form_dict["site"] = site or (events[0].get("site") if events else None)
	_bulk_ingest(events)


@rate_limit(key="site", limit=get_rate_limit, seconds=60)
def _bulk_ingest(events):
	check_auth()
	failed = []
	for event in events:
		try:
			event = frappe._dict(event)
			enqueue_event(
				event_name=event.event_name,
				captured_at=event.captured_at,
				site=event.site,
				app=event.app,
				user=event.user,
				team=event.team,
				properties=event.properties,
			)
		except Exception as e:
			failed.append(
				{
					"event": {
						"event_name": event.event_name,
						"site": event.site,
						"app": event.app,
						"user": event.user,
					},
					"error": str(e),
				}
			)

	if failed:
		logger.error(
			{
				"request_ip": frappe.local.request_ip,
				"events": failed,
				"error": "Failed to insert some events",
			}
		)
		frappe.throw("Failed to insert some events", frappe.ValidationError)


@frappe.whitelist(allow_guest=True, methods=["POST"])
def identify(user, properties=None):
	check_auth()
	try:
		upsert_person(user, properties)
	except Exception as e:
		logger.error(
			{
				"request_ip": frappe.local.request_ip,
				"identify": {"user": user},
				"error": str(e),
			}
		)
		raise e


@frappe.whitelist(allow_guest=True, methods=["POST"])
def alias(previous_id, user):
	check_auth()
	try:
		upsert_alias(previous_id, user)
	except Exception as e:
		logger.error(
			{
				"request_ip": frappe.local.request_ip,
				"alias": {"previous_id": previous_id, "user": user},
				"error": str(e),
			}
		)
		raise e


def _merge_properties(existing, incoming):
	"""Merge incoming attributes over existing (later values win, others kept)."""
	if isinstance(existing, str):
		existing = frappe.parse_json(existing)
	if isinstance(incoming, str):
		incoming = frappe.parse_json(incoming)
	return {**(existing or {}), **(incoming or {})}


def upsert_person(user, properties=None):
	"""Create or update a Pulse Person profile, merging incoming attributes.

	Profiles are mutable state (set on change), not append-only events — so a
	later `identify` updates the given keys and keeps the rest.
	"""
	if not user:
		frappe.throw("user is required", frappe.ValidationError)

	if frappe.db.exists("Pulse Person", user):
		doc = frappe.get_doc("Pulse Person", user)
		doc.properties = _merge_properties(doc.properties, properties)
		doc.save(ignore_permissions=True)
	else:
		doc = frappe.get_doc(
			{"doctype": "Pulse Person", "user": user, "properties": _merge_properties({}, properties)}
		)
		doc.insert(ignore_permissions=True)
	return doc


def upsert_alias(previous_id, user):
	"""Record that a previous (anonymous) user id maps to a known user.

	Stores the mapping; re-attribution of historical events to `user` is resolved
	downstream in Insights (events joined to this alias map at query time), the way
	PostHog resolves merged persons. Pulse never rewrites historical event rows.
	"""
	if not previous_id or not user:
		frappe.throw("previous_id and user are required", frappe.ValidationError)

	if frappe.db.exists("Pulse Alias", previous_id):
		doc = frappe.get_doc("Pulse Alias", previous_id)
		doc.user = user
		doc.save(ignore_permissions=True)
	else:
		doc = frappe.get_doc({"doctype": "Pulse Alias", "previous_id": previous_id, "user": user})
		doc.insert(ignore_permissions=True)
	return doc


def check_auth():
	api_key = frappe.get_single("Pulse Settings").get_password("api_key")
	if not api_key:
		logger.error("Pulse API key is not configured")
		frappe.throw("Pulse API key is not configured", frappe.PermissionError)

	headers = frappe.request.headers
	header_name = "X-Pulse-API-Key"
	req_api_key = headers.get(header_name)
	if not req_api_key:
		logger.error(
			{
				"request_ip": frappe.local.request_ip,
				"error": f"{header_name} header is missing",
			}
		)
		frappe.throw(f"{header_name} header is missing", frappe.PermissionError)

	if req_api_key != api_key:
		logger.error(
			{
				"request_ip": frappe.local.request_ip,
				"error": f"Invalid {header_name}",
			}
		)
		frappe.throw(f"Invalid {header_name}", frappe.PermissionError)
