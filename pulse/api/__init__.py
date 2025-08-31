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
		logger.error(f"Failed to insert event: {event_name}, Error: {e}")
		raise e


@frappe.whitelist(allow_guest=True, methods=["POST"])
@rate_limit(limit=get_rate_limit, seconds=60 * 60)
def bulk_ingest(events):
	if not isinstance(events, list):
		frappe.throw("Events must be a list", frappe.ValidationError)

	check_auth()
	for event in events:
		try:
			doc = frappe.new_doc("Pulse Event")
			doc.event_name = event.get("event_name")
			doc.captured_at = event.get("captured_at")
			doc.site = event.get("site")
			doc.user = event.get("user")
			doc.app = event.get("app")
			doc.properties = event.get("properties") or {}
			doc.validate()
			doc.db_insert()
		except Exception as e:
			logger.error(f"Failed to insert event: {event}, Error: {e}")


def check_auth():
	api_key = frappe.get_single("Pulse Settings").get_password("api_key")
	if not api_key:
		logger.error("Pulse API key is not configured")
		frappe.throw("Pulse API key is not configured", frappe.PermissionError)

	headers = frappe.request.headers
	header_name = "X-Pulse-API-Key"
	req_api_key = headers.get(header_name)
	if not req_api_key:
		logger.error(f"{header_name} header is missing")
		frappe.throw(f"{header_name} header is missing", frappe.PermissionError)

	if req_api_key != api_key:
		logger.error(f"Invalid {header_name}")
		frappe.throw(f"Invalid {header_name}", frappe.PermissionError)
