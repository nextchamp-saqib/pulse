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


def decode(data):
	if isinstance(data, bytes):
		return data.decode("utf-8")
	elif isinstance(data, dict):
		return {decode(k): decode(v) for k, v in data.items()}
	elif isinstance(data, list | tuple | set):
		return [decode(item) for item in data]
	else:
		return data


def pretty_bytes(size):
	if size is None:
		return "N/A"
	if size < 1024:
		return f"{size} B"
	elif size < 1024**2:
		return f"{size / 1024:.2f} KB"
	elif size < 1024**3:
		return f"{size / (1024**2):.2f} MB"
	else:
		return f"{size / (1024**3):.2f} GB"
