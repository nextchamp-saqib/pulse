import frappe
from frappe.utils.logger import set_log_level

from .constants import LOGGER_NAME

_logger = None


def get_logger():
	global _logger
	if _logger is None:
		set_log_level("DEBUG")
		_logger = frappe.logger(LOGGER_NAME)
	return _logger
