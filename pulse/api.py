import frappe
from frappe.rate_limiter import rate_limit

from .constants import API_RATE_LIMIT, API_RATE_LIMIT_SECONDS
from .logger import get_logger
from .processor import EventProcessor
from .stream import RedisStream

logger = get_logger()


@frappe.whitelist(allow_guest=True, methods=["POST"])
@rate_limit(limit=API_RATE_LIMIT, seconds=API_RATE_LIMIT_SECONDS)
def track_event(site_name, event, app_name, app_version, timestamp, **kwargs):
	try:
		stream = RedisStream()
		stream.add(
			{
				"site_name": site_name,
				"event": event,
				"app_name": app_name,
				"app_version": app_version,
				"timestamp": timestamp,
				**kwargs,
			}
		)

		return {"status": "success", "message": "Event tracked successfully"}
	except Exception as e:
		return {
			"status": "error",
			"message": f"Failed to track event: {e!s}",
		}


__all__ = [
	"EventProcessor",
	"RedisStream",
	"track_event",
]
