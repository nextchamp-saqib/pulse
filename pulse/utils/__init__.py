import frappe

from pulse.logger import get_logger

logger = get_logger()


def log_error():
	def decorator(func):
		def wrapper(*args, **kwargs):
			try:
				return func(*args, **kwargs)
			except Exception as e:
				traceback = frappe.as_unicode(frappe.get_traceback(with_context=True))
				logger.error(
					{
						"function": f"{func.__module__}.{func.__qualname__}",
						"args": args,
						"kwargs": kwargs,
						"error": str(e),
						"traceback": traceback,
					}
				)
				raise e

		return wrapper

	return decorator
