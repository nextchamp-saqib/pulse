import frappe
from frappe.rate_limiter import rate_limit

from pulse.logger import get_logger

from ..constants import API_RATE_LIMIT, API_RATE_LIMIT_SECONDS

logger = get_logger()


@frappe.whitelist(allow_guest=True, methods=["POST"])
@rate_limit(limit=API_RATE_LIMIT, seconds=API_RATE_LIMIT_SECONDS)
def ingest(event_name, subject_id, subject_type, captured_at, props=None):
	check_auth()

	try:
		doc = frappe.new_doc("Pulse Event")
		doc.event_name = event_name
		doc.subject_id = subject_id
		doc.subject_type = subject_type
		doc.captured_at = captured_at
		doc.props = props or {}
		doc.validate()
		doc.db_insert()
	except Exception as e:
		logger.error(f"Failed to insert event: {event_name}, Error: {e}")


@frappe.whitelist(allow_guest=True, methods=["POST"])
@rate_limit(limit=API_RATE_LIMIT, seconds=API_RATE_LIMIT_SECONDS)
def bulk_ingest(events):
	check_auth()

	if not isinstance(events, list):
		frappe.throw("Events must be a list", frappe.ValidationError)

	for event in events:
		try:
			doc = frappe.new_doc("Pulse Event")
			doc.event_name = event.get("event_name")
			doc.subject_id = event.get("subject_id")
			doc.subject_type = event.get("subject_type")
			doc.captured_at = event.get("captured_at")
			doc.props = event.get("props") or {}
			doc.validate()
			doc.db_insert()
		except Exception as e:
			logger.error(f"Failed to insert event: {event}, Error: {e}")


def check_auth():
	api_key = frappe.get_single("Pulse Settings").get_password("api_key")
	if not api_key:
		frappe.throw("Pulse API key is not configured", frappe.PermissionError)

	headers = frappe.request.headers
	header_name = "X-Pulse-API-Key"
	if not headers.get(header_name):
		frappe.throw(f"{header_name} header is missing", frappe.PermissionError)

	token = headers.get(header_name)
	if not token:
		frappe.throw("Authorization token is missing", frappe.PermissionError)

	if token != api_key:
		frappe.throw("Invalid Authorization token", frappe.PermissionError)
