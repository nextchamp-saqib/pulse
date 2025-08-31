import frappe
from frappe.rate_limiter import rate_limit

from pulse.logger import get_logger

logger = get_logger()


def get_rate_limit():
	return frappe.get_single_value("Pulse Settings", "rate_limit") or 10


@frappe.whitelist(allow_guest=True, methods=["POST"])
@rate_limit(limit=get_rate_limit, seconds=60 * 60)
def ingest(event_name, captured_at, site=None, app=None, user=None, properties=None):
	check_auth()

	try:
		doc = frappe.new_doc("Pulse Event")
		doc.event_name = event_name
		doc.captured_at = captured_at
		doc.site = site
		doc.app = app
		doc.user = user
		doc.properties = properties or {}
		doc.validate()
		doc.db_insert()
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
@rate_limit(limit=get_rate_limit, seconds=60 * 60)
def bulk_ingest(events):
	if not isinstance(events, list):
		frappe.throw("Events must be a list", frappe.ValidationError)

	check_auth()
	failed = []
	for event in events:
		try:
			event = frappe._dict(event)
			doc = frappe.new_doc("Pulse Event")
			doc.event_name = event.event_name
			doc.captured_at = event.captured_at
			doc.site = event.site
			doc.user = event.user
			doc.app = event.app
			doc.properties = event.properties or {}
			doc.validate()
			doc.db_insert()
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
