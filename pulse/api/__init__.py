import frappe
from frappe.rate_limiter import rate_limit

from pulse.logger import get_logger

from ..constants import API_RATE_LIMIT, API_RATE_LIMIT_SECONDS

logger = get_logger()


@frappe.whitelist(allow_guest=True, methods=["POST"])
@rate_limit(limit=API_RATE_LIMIT, seconds=API_RATE_LIMIT_SECONDS)
def ingest(events):
	check_auth()

	failures = []
	for event in events:
		try:
			doc = frappe.new_doc("Pulse Event")
			doc.update(event)
			doc.data = {
				k: v
				for k, v in event.items()
				if k
				not in [
					"event_name",
					"site",
					"timestamp",
					"app",
					"app_version",
					"frappe_version",
				]
			}
			doc.db_insert()
		except Exception as e:
			logger.error(f"Failed to insert event: {event}, Error: {e}")
			failures.append(event)

	if failures:
		frappe.throw(f"{len(failures)} events failed to insert. Check logs for details.")


def check_auth():
	api_key = frappe.get_single_value("Pulse Settings", "api_key")
	if not api_key:
		frappe.throw("Pulse API key is not configured", frappe.PermissionError)

	# check headers
	headers = frappe.request.headers
	if not headers.get("Authorization"):
		frappe.throw("Authorization header is missing", frappe.PermissionError)

	# check token
	token = headers.get("Authorization").split(" ")[-1]
	if not token:
		frappe.throw("Authorization token is missing", frappe.PermissionError)

	# validate token
	if token != api_key:
		frappe.throw("Invalid Authorization token", frappe.PermissionError)


def validate_events(events):
	required = ("site", "name", "timestamp")
	for event in events:
		invalid = [k for k in required if not event.get(k)]
		if invalid:
			frappe.throw(f"Event failed validation. Missing/empty: {invalid}")
