import os

import frappe
import ibis
from frappe.model.utils import is_virtual_doctype
from frappe.utils import get_files_path

from pulse.logger import get_logger

logger = get_logger()


def log_error():
	def decorator(func):
		def wrapper(*args, **kwargs):
			try:
				return func(*args, **kwargs)
			except Exception as e:
				traceback = frappe.as_unicode(frappe.get_traceback(with_context=True))
				logger.error({
					"function": f"{func.__module__}.{func.__qualname__}",
					"args": args,
					"kwargs": kwargs,
					"error": str(e),
					"traceback": traceback,
				})
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


def get_etl_batch(doctype, checkpoint=None, batch_size=1000):
	if is_virtual_doctype(doctype):
		from frappe.model.base_document import get_controller

		controller = get_controller(doctype)
		if not hasattr(controller, "get_etl_batch"):
			raise NotImplementedError

		return frappe.call(controller.get_etl_batch, checkpoint=checkpoint, batch_size=batch_size)

	creation_key, id_key = "creation", "name"
	filters = None
	if checkpoint:
		filters = [[creation_key, ">", checkpoint]]

	return frappe.get_all(
		doctype,
		fields=["*"],
		filters=filters,
		limit=batch_size,
		order_by=f"{creation_key}, {id_key}",
	)


@log_error()
def get_warehouse_connection(readonly=True):
	db_path = get_db_path()
	conn = ibis.duckdb.connect()
	conn.raw_sql("INSTALL ducklake;")
	conn.raw_sql(f"ATTACH 'ducklake:{db_path}' AS warehouse {'(READ_ONLY)' if readonly else ''};")
	conn.raw_sql("USE warehouse;")
	return conn


def get_db_path():
	base = os.path.realpath(get_files_path(is_private=1))
	folder = os.path.join(base, "duckdb")
	os.makedirs(folder, exist_ok=True)
	return os.path.join(folder, "warehouse.ducklake")
